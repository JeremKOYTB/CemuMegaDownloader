package main

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cemumegadownloader "CemuMegaDownloader"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
)

const (
	defaultConcurrentTitles = 3
	barWidth                = 20
)

type KeyCode int

const (
	KeyNone KeyCode = iota
	KeyUp
	KeyDown
	KeySpace
	KeyEnter
	KeyToggleAll
	KeyQuit
)

type TitleItem struct {
	TitleID  string
	Name     string
	Path     string
	Selected bool
}

type SpeedSample struct {
	t     time.Time
	bytes int64
}

type SpeedEstimator struct {
	samples   []SpeedSample
	speedMBps float64
}

func newSpeedEstimator() *SpeedEstimator {
	return &SpeedEstimator{
		samples: []SpeedSample{{t: time.Now(), bytes: 0}},
	}
}

func (se *SpeedEstimator) Update(currentBytes int64) float64 {
	now := time.Now()
	se.samples = append(se.samples, SpeedSample{t: now, bytes: currentBytes})

	cutoff := now.Add(-3 * time.Second)
	idx := 0
	for i, s := range se.samples {
		if s.t.After(cutoff) {
			idx = i
			break
		}
	}
	if idx > 0 {
		se.samples = se.samples[idx-1:]
	}

	if len(se.samples) >= 2 {
		first := se.samples[0]
		last := se.samples[len(se.samples)-1]
		dt := last.t.Sub(first.t).Seconds()
		diff := last.bytes - first.bytes
		if dt >= 0.5 && diff >= 0 {
			instant := (float64(diff) / (1024 * 1024)) / dt
			if se.speedMBps <= 0 {
				se.speedMBps = instant
			} else {
				se.speedMBps = 0.25*instant + 0.75*se.speedMBps
			}
		}
	}
	return se.speedMBps
}

type ProgressReporterCLI struct {
	mu              sync.Mutex
	totalToDownload int64
	totalDownloaded atomic.Int64
	startTime       time.Time
	lastPrintNano   atomic.Int64
	totalTitles     int
	completedTitles atomic.Int32
	activeTitles    atomic.Int32
	exactSizeKnown  bool
	speedEstimator  *SpeedEstimator
}

func NewProgressReporterCLI() *ProgressReporterCLI {
	return &ProgressReporterCLI{
		speedEstimator: newSpeedEstimator(),
		startTime:      time.Now(),
	}
}

func (pr *ProgressReporterCLI) SetBatchTotals(totalTitles int, totalBytes int64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.totalToDownload = totalBytes
	pr.totalDownloaded.Store(0)
	pr.startTime = time.Now()
	pr.lastPrintNano.Store(0)
	pr.totalTitles = totalTitles
	pr.completedTitles.Store(0)
	pr.activeTitles.Store(0)
	pr.exactSizeKnown = totalBytes > 0
	pr.speedEstimator = newSpeedEstimator()
}

func (pr *ProgressReporterCLI) LogLine(format string, args ...any) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	fmt.Print("\r\033[2K")
	fmt.Printf(format+"\n", args...)
	pr.renderLocked(true)
}

func (pr *ProgressReporterCLI) ClearLine() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	fmt.Print("\r\033[2K")
}

func formatBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int(d.Seconds())
	h := s / 3600
	m := (s % 3600) / 60
	sec := s % 60
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
	}
	return fmt.Sprintf("%02d:%02d", m, sec)
}

func makeProgressBar(percentage float64, width int) string {
	if percentage < 0 {
		percentage = 0
	}
	if percentage > 100 {
		percentage = 100
	}
	filled := int((percentage / 100.0) * float64(width))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func (pr *ProgressReporterCLI) AddBytes(n int64) {
	curr := pr.totalDownloaded.Add(n)
	if curr < 0 {
		pr.totalDownloaded.Store(0)
	}
	now := time.Now().UnixNano()
	last := pr.lastPrintNano.Load()
	if now-last < int64(80*time.Millisecond) {
		return
	}
	if pr.lastPrintNano.CompareAndSwap(last, now) {
		pr.mu.Lock()
		pr.renderLocked(false)
		pr.mu.Unlock()
	}
}

func (pr *ProgressReporterCLI) TitleStarted() {
	pr.activeTitles.Add(1)
	pr.mu.Lock()
	pr.renderLocked(false)
	pr.mu.Unlock()
}

func (pr *ProgressReporterCLI) TitleFinished(success bool) {
	pr.activeTitles.Add(-1)
	if success {
		pr.completedTitles.Add(1)
	}
	pr.mu.Lock()
	pr.renderLocked(true)
	pr.mu.Unlock()
}

func (pr *ProgressReporterCLI) renderLocked(force bool) {
	totalDown := pr.totalDownloaded.Load()
	if totalDown < 0 {
		totalDown = 0
	}
	compTitles := int(pr.completedTitles.Load())
	actTitles := int(pr.activeTitles.Load())
	if actTitles < 0 {
		actTitles = 0
	}

	elapsed := time.Since(pr.startTime)
	speedMBps := pr.speedEstimator.Update(totalDown)
	if speedMBps <= 0 && elapsed.Seconds() > 0.1 {
		speedMBps = (float64(totalDown) / (1024 * 1024)) / elapsed.Seconds()
	}

	var percentage float64
	remainingStr := "--:--"

	if pr.totalToDownload > 0 {
		percentage = (float64(totalDown) / float64(pr.totalToDownload)) * 100.0
		if percentage > 100.0 {
			percentage = 100.0
		}
		if totalDown >= pr.totalToDownload {
			remainingStr = "00:00"
		} else if speedMBps > 0.05 {
			effSpeed := speedMBps
			if elapsed.Seconds() > 5.0 {
				avgSpeed := (float64(totalDown) / (1024 * 1024)) / elapsed.Seconds()
				if avgSpeed > 0.01 {
					effSpeed = 0.7*speedMBps + 0.3*avgSpeed
				}
			}
		remBytes := float64(pr.totalToDownload - totalDown)
		remSec := remBytes / (effSpeed * 1024 * 1024)
		remainingStr = formatDuration(time.Duration(remSec * float64(time.Second)))
		}
	} else if pr.totalTitles > 0 {
		percentage = (float64(compTitles) / float64(pr.totalTitles)) * 100.0
	}

	totalStr := formatBytes(pr.totalToDownload)
	if !pr.exactSizeKnown {
		totalStr = "?"
	}

	line := fmt.Sprintf("[%d/%d done | %d active] [%s] %5.1f%% | %s / %s | %.2f MB/s | %s < %s",
		compTitles,
		pr.totalTitles,
		actTitles,
		makeProgressBar(percentage, barWidth),
		percentage,
		formatBytes(totalDown),
		totalStr,
		speedMBps,
		formatDuration(elapsed),
		remainingStr,
	)

	termW, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || termW <= 10 {
		termW = 80
	}
	maxLen := max(20, termW-1)

	r := []rune(line)
	if len(r) > maxLen {
		line = string(r[:maxLen])
	}

	fmt.Printf("\r\033[2K%s", line)
}

func newHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   256,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,
			ReadBufferSize:        256 * 1024,
			WriteBufferSize:       64 * 1024,
		},
	}
}

func cleanPathInput(raw string) string {
	p := strings.TrimSpace(raw)
	p = strings.Trim(p, "\"'`")
	p = strings.TrimSpace(p)
	p = os.ExpandEnv(p)
	if strings.HasPrefix(p, "~") {
		if home, err := os.UserHomeDir(); err == nil {
			p = filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return filepath.Clean(p)
}

func checkDirectoryWritable(dir string) error {
	cleaned := cleanPathInput(dir)
	if err := os.MkdirAll(cleaned, 0755); err != nil {
		return err
	}
	testFile := filepath.Join(cleaned, fmt.Sprintf(".perm_test_%d", time.Now().UnixNano()))
	f, err := os.Create(testFile)
	if err != nil {
		return err
	}
	_ = f.Close()
	_ = os.Remove(testFile)
	return nil
}

func extractMLCPathFromSettingsXML(xmlPath string) string {
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		return ""
	}

	type XMLSettings struct {
		XMLName xml.Name `xml:"content"`
		MLCPath string   `xml:"mlc_path"`
	}

	var s XMLSettings
	if err := xml.Unmarshal(data, &s); err == nil && strings.TrimSpace(s.MLCPath) != "" {
		return cleanPathInput(s.MLCPath)
	}

	type SimpleXML struct {
		MLCPath string `xml:"mlc_path"`
	}
	var s2 SimpleXML
	if err := xml.Unmarshal(data, &s2); err == nil && strings.TrimSpace(s2.MLCPath) != "" {
		return cleanPathInput(s2.MLCPath)
	}

	lines := strings.Split(string(data), "\n")
	for _, l := range lines {
		if strings.Contains(l, "<mlc_path>") && strings.Contains(l, "</mlc_path>") {
			start := strings.Index(l, "<mlc_path>") + len("<mlc_path>")
			end := strings.Index(l, "</mlc_path>")
			if start < end {
				return cleanPathInput(l[start:end])
			}
		}
	}

	return ""
}

func detectCemuDirectories() (string, string) {
	homeDir, _ := os.UserHomeDir()
	var candidates []string

	switch runtime.GOOS {
	case "windows":
		if appdata := os.Getenv("APPDATA"); appdata != "" {
			candidates = append(candidates, filepath.Join(appdata, "Cemu"))
			if entries, err := os.ReadDir(appdata); err == nil {
				for _, e := range entries {
					if e.IsDir() && strings.Contains(strings.ToLower(e.Name()), "cemu") && e.Name() != "Cemu" {
						candidates = append(candidates, filepath.Join(appdata, e.Name()))
					}
				}
			}
		}
		if local := os.Getenv("LOCALAPPDATA"); local != "" {
			candidates = append(candidates, filepath.Join(local, "Cemu"))
		}
		if homeDir != "" {
			dlDir := filepath.Join(homeDir, "Downloads")
			if entries, err := os.ReadDir(dlDir); err == nil {
				for _, e := range entries {
					if e.IsDir() && strings.Contains(strings.ToLower(e.Name()), "cemu") {
						cPath := filepath.Join(dlDir, e.Name())
						candidates = append(candidates, cPath)
						if subEntries, err := os.ReadDir(cPath); err == nil {
							for _, se := range subEntries {
								if se.IsDir() && strings.Contains(strings.ToLower(se.Name()), "cemu") {
									candidates = append(candidates, filepath.Join(cPath, se.Name()))
								}
							}
						}
					}
				}
			}
		}
	case "darwin":
		if homeDir != "" {
			candidates = append(candidates, filepath.Join(homeDir, "Library", "Application Support", "Cemu"))
		}
	default:
		if homeDir != "" {
			candidates = append(candidates,
				filepath.Join(homeDir, ".var", "app", "info.cemu.Cemu", "data", "cemu"),
				filepath.Join(homeDir, ".var", "app", "info.cemu.Cemu", "config", "cemu"),
			)
		}
		if dataHome := os.Getenv("XDG_DATA_HOME"); dataHome != "" {
			candidates = append(candidates, filepath.Join(dataHome, "Cemu"))
		} else if homeDir != "" {
			candidates = append(candidates, filepath.Join(homeDir, ".local", "share", "Cemu"))
		}
		if cfgHome := os.Getenv("XDG_CONFIG_HOME"); cfgHome != "" {
			candidates = append(candidates, filepath.Join(cfgHome, "Cemu"))
		} else if homeDir != "" {
			candidates = append(candidates, filepath.Join(homeDir, ".config", "Cemu"))
		}
	}

	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates, filepath.Join(exeDir, "portable"), exeDir)
	}
	candidates = append(candidates, "portable", ".")

	for _, c := range candidates {
		xmlCandidates := []string{
			filepath.Join(c, "settings.xml"),
			filepath.Join(strings.Replace(c, filepath.Join("data", "cemu"), filepath.Join("config", "cemu"), 1), "settings.xml"),
			filepath.Join(strings.Replace(c, filepath.Join(".local", "share"), ".config", 1), "settings.xml"),
		}
		for _, xmlFile := range xmlCandidates {
			if info, err := os.Stat(xmlFile); err == nil && !info.IsDir() {
				if mlcCustom := extractMLCPathFromSettingsXML(xmlFile); mlcCustom != "" {
					return c, mlcCustom
				}
				return c, filepath.Join(c, "mlc01")
			}
		}

		if info, err := os.Stat(filepath.Join(c, "log.txt")); err == nil && !info.IsDir() {
			return c, filepath.Join(c, "mlc01")
		}

		for _, name := range []string{"Cemu.exe", "cemu.exe", "Cemu", "cemu"} {
			if info, err := os.Stat(filepath.Join(c, name)); err == nil && !info.IsDir() {
				return c, filepath.Join(c, "mlc01")
			}
		}
	}

	return "", ""
}

func inspectCustomCemuPath(p string) (string, string) {
	cleaned := cleanPathInput(p)
	for _, dir := range []string{cleaned, filepath.Dir(cleaned)} {
		for _, name := range []string{"settings.xml", "Cemu.exe", "cemu.exe", "Cemu", "cemu"} {
			if info, err := os.Stat(filepath.Join(dir, name)); err == nil && !info.IsDir() {
				return dir, filepath.Join(dir, "mlc01")
			}
		}
	}
	return "", ""
}

func ensureStandardMLCDirectories(mlcRoot string) {
	for _, sub := range []string{
		"sys/config",
		"sys/import",
		"sys/update",
		"sys/title",
		"usr/title",
		"usr/save/system/act",
	} {
		_ = os.MkdirAll(filepath.Join(mlcRoot, filepath.FromSlash(sub)), 0755)
	}
}

func detectCemuRegion(cemuDir string) string {
	if cemuDir == "" {
		return ""
	}
	xmlFile := filepath.Join(cemuDir, "settings.xml")
	if data, err := os.ReadFile(xmlFile); err == nil {
		s := string(data)
		if strings.Contains(s, "10040200") || strings.Contains(s, "10047200") {
			return "EUR"
		}
		if strings.Contains(s, "10040100") || strings.Contains(s, "10047100") {
			return "USA"
		}
		if strings.Contains(s, "10040000") || strings.Contains(s, "10047000") {
			return "JPN"
		}
	}
	sysDirs := []string{
		filepath.Join(cemuDir, "mlc01", "sys", "title", "00050010"),
	}
	if mlcCustom := extractMLCPathFromSettingsXML(xmlFile); mlcCustom != "" {
		sysDirs = append([]string{filepath.Join(mlcCustom, "sys", "title", "00050010")}, sysDirs...)
	}
	for _, sysDir := range sysDirs {
		if entries, err := os.ReadDir(sysDir); err == nil {
			for _, e := range entries {
				if e.Name() == "10040200" {
					return "EUR"
				}
				if e.Name() == "10040100" {
					return "USA"
				}
				if e.Name() == "10040000" {
					return "JPN"
				}
			}
		}
	}
	return ""
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func setupCafeLibs(mlcRoot, cemuDir, targetBaseDir string) string {
	osv10Code := filepath.Join(mlcRoot, "sys", "title", "00050010", "1000400a", "code")
	if info, err := os.Stat(osv10Code); err != nil || !info.IsDir() {
		return ""
	}

	rpls := []string{
		"snd_user.rpl",
		"snduser2.rpl",
		"swkbd.rpl",
		"nsyskbd.rpl",
		"erreula.rpl",
		"drmapp.rpl",
		"nn_sl.rpl",
	}

	var targets []string
	if cemuDir != "" {
		targets = append(targets, filepath.Join(cemuDir, "cafeLibs"))
	}

	baseClean := cleanPathInput(targetBaseDir)
	localDest := filepath.Join(baseClean, "cafeLibs")
	if strings.HasSuffix(strings.ToLower(baseClean), "mlc01") {
		localDest = filepath.Join(filepath.Dir(baseClean), "cafeLibs")
	}

	found := false
	for _, t := range targets {
		if filepath.Clean(t) == filepath.Clean(localDest) {
			found = true
			break
		}
	}
	if !found {
		targets = append(targets, localDest)
	}

	var primary string
	for _, dest := range targets {
		if err := os.MkdirAll(dest, 0755); err != nil {
			continue
		}
		copied := 0
		for _, rpl := range rpls {
			src := filepath.Join(osv10Code, rpl)
			if _, err := os.Stat(src); err == nil {
				if err := copyFile(src, filepath.Join(dest, rpl)); err == nil {
					copied++
				}
			}
		}
		if copied > 0 && primary == "" {
			primary = dest
		}
	}
	return primary
}

func configureCemuGameProfile(cemuDir, region string, reader *bufio.Reader) bool {
	if cemuDir == "" {
		return false
	}

	menuTIDs := map[string]string{
		"EUR": "0005001010040200",
		"USA": "0005001010040100",
		"JPN": "0005001010040000",
	}
	menuTID, ok := menuTIDs[region]
	if !ok {
		return false
	}

	profilesDir := filepath.Join(cemuDir, "gameProfiles")
	if err := os.MkdirAll(profilesDir, 0755); err != nil {
		return false
	}

	profilePath := filepath.Join(profilesDir, menuTID+".ini")
	bakPath := profilePath + ".bak"

	if data, err := os.ReadFile(profilePath); err == nil {
		shouldBackup := true
		if _, errBak := os.Stat(bakPath); errBak == nil {
			fmt.Printf("\n[Notice] Existing profile backup detected: %s\n", bakPath)
			if reader != nil && !askYesNo(reader, "Do you want to overwrite this existing backup? [y/N]: ", false) {
				shouldBackup = false
				fmt.Println("[Info] Existing backup preserved.")
			}
		}
		if shouldBackup {
			_ = os.WriteFile(bakPath, data, 0644)
		}

		content := string(data)
		if strings.Contains(content, "loadSharedLibraries") {
			re := regexp.MustCompile(`(?i)loadSharedLibraries\s*=\s*(false|true)`)
			content = re.ReplaceAllString(content, "loadSharedLibraries = true")
		} else if strings.Contains(content, "[General]") {
			content = strings.Replace(content, "[General]", "[General]\nloadSharedLibraries = true", 1)
		} else {
			content = "[General]\nloadSharedLibraries = true\n\n" + content
		}
		return os.WriteFile(profilePath, []byte(content), 0644) == nil
	}

	return os.WriteFile(profilePath, []byte("[General]\nloadSharedLibraries = true\n"), 0644) == nil
}

func resolveMLCDestPath(baseDir, titleID string) string {
	clean := cemumegadownloader.NormalizeTID(titleID)
	high := clean
	low := ""
	if len(clean) >= 16 {
		high = clean[:8]
		low = clean[8:]
	}

	subFolder := "sys"
	if high == "0005000b" || high == "00050000" || high == "00050002" || high == "0005000c" || (high == "0005000e" && clean != "0005000e1fbf1000") {
		subFolder = "usr"
	}

	baseClean := cleanPathInput(baseDir)
	if strings.HasSuffix(strings.ToLower(baseClean), "mlc01") {
		if low != "" {
			return filepath.Join(baseClean, subFolder, "title", high, low)
		}
		return filepath.Join(baseClean, subFolder, "title", clean)
	}

	if low != "" {
		return filepath.Join(baseClean, "mlc01", subFolder, "title", high, low)
	}
	return filepath.Join(baseClean, "mlc01", subFolder, "title", clean)
}

func ensureTitlesJSONData(client *http.Client) ([]byte, error) {
	candidates := []string{"titles.json"}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "titles.json"))
	}

	for _, p := range candidates {
		if data, err := os.ReadFile(p); err == nil && len(data) > 64 {
			var cat cemumegadownloader.TitlesCatalog
			if json.Unmarshal(data, &cat) == nil && len(cat.EUR) > 0 && cat.EUR[0].Name != "" && cat.EUR[0].Name != cat.EUR[0].TitleID {
				return data, nil
			}
		}
	}

	fmt.Println("[Info] Generating titles.json...")
	return cemumegadownloader.GenerateTitlesJSON(client, "titles.json")
}

func LoadTitlesForRegion(region, baseDir string, client *http.Client) ([]TitleItem, error) {
	data, err := ensureTitlesJSONData(client)
	if err != nil {
		return nil, err
	}

	var cat cemumegadownloader.TitlesCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		return nil, fmt.Errorf("JSON parse error: %w", err)
	}

	var regionList []cemumegadownloader.TitleDefinition
	switch region {
	case "EUR":
		regionList = append(regionList, cat.EUR...)
	case "USA":
		regionList = append(regionList, cat.USA...)
	case "JPN":
		regionList = append(regionList, cat.JPN...)
	}
	regionList = append(regionList, cat.All...)

	seen := make(map[string]struct{})
	items := make([]TitleItem, 0, len(regionList))

	for _, entry := range regionList {
		norm := cemumegadownloader.NormalizeTID(entry.TitleID)
		if norm == "" || norm == "dummy" {
			continue
		}
		if _, ok := seen[norm]; !ok {
			seen[norm] = struct{}{}
			name := cemumegadownloader.CleanTitleName(entry.Name)
			if name == "" {
				name = norm
			}
			items = append(items, TitleItem{
				TitleID:  norm,
				Name:     name,
				Path:     resolveMLCDestPath(baseDir, norm),
				Selected: true,
			})
		}
	}

	return items, nil
}

func fitMiddle(s string, maxLen int) string {
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return string(r[:maxLen])
	}
	half := (maxLen - 3) / 2
	return string(r[:half]) + "..." + string(r[len(r)-(maxLen-3-half):])
}

func SelectTitlesInteractive(items []TitleItem, region string) ([]TitleItem, bool) {
	total := len(items)
	if total == 0 {
		return items, false
	}

	restoreRaw := enterRawTerminal()
	defer restoreRaw()

	flushStdin()
	cursor := 0
	scrollOffset := 0

	fmt.Print("\033[?1049h\033[?25l")
	defer fmt.Print("\033[?1049l\033[?25h")

	maxNameLen := 15
	for _, it := range items {
		if l := len([]rune(it.Name)); l > maxNameLen {
			maxNameLen = l
		}
	}

	lastW, lastH := 0, 0
	needRedraw := true

	for {
		termW, termH, err := term.GetSize(int(os.Stdout.Fd()))
		if err != nil || termW < 40 {
			termW = 80
		}
		if termH < 10 {
			termH = 24
		}

		if termW != lastW || termH != lastH {
			lastW = termW
			lastH = termH
			needRedraw = true
		}

		innerW := max(36, termW-2)
		pageSize := max(4, min(total, termH-8))

		cursor = max(0, min(cursor, total-1))

		if cursor < scrollOffset {
			scrollOffset = cursor
			needRedraw = true
		}
		if cursor >= scrollOffset+pageSize {
			scrollOffset = cursor - pageSize + 1
			needRedraw = true
		}
		scrollOffset = max(0, scrollOffset)
		if total > pageSize && scrollOffset > total-pageSize {
			scrollOffset = total - pageSize
		}

		if needRedraw {
			selectedCount := 0
			for _, it := range items {
				if it.Selected {
					selectedCount++
				}
			}

			fmt.Print("\033[H")

			printBordered := func(content string) {
				r := []rune(content)
				if len(r) > innerW {
					r = r[:innerW]
				}
				pad := max(0, innerW-len(r))
				fmt.Printf("\r\033[2K║%s%s║\n", string(r), strings.Repeat(" ", pad))
			}

			fmt.Printf("\r\033[2K╔%s╗\n", strings.Repeat("═", innerW))
			printBordered(fmt.Sprintf(" Title Selection: %-4s", region))
			printBordered(" [↑/↓] Navigate | [SPACE] Toggle | [A] Toggle All")
			printBordered(" [ENTER] Start Download | [Q/ESC] Cancel and Exit")

			showPathColumn := innerW >= 24+maxNameLen+4+15

			for i := 0; i < pageSize; i++ {
				idx := scrollOffset + i
				if idx >= total {
					break
				}

				prefix := "  "
				if idx == cursor {
					prefix = "> "
				}

				box := "[ ] "
				if items[idx].Selected {
					box = "[X] "
				}

				if showPathColumn {
					nameW := maxNameLen
					availPath := max(10, innerW-24-nameW-4)
					nameStr := fitMiddle(items[idx].Name, nameW)
					namePad := max(0, nameW-len([]rune(nameStr)))
					pathStr := fitMiddle(items[idx].Path, availPath)
					pathPad := max(0, availPath-len([]rune(pathStr)))
					rowContent := fmt.Sprintf("%s%s%s  %s%s -> %s%s",
						prefix, box, items[idx].TitleID, nameStr, strings.Repeat(" ", namePad), pathStr, strings.Repeat(" ", pathPad))
					printBordered(rowContent)
				} else {
					availName := innerW - 24
					nameStr := fitMiddle(items[idx].Name, availName)
					namePad := max(0, availName-len([]rune(nameStr)))
					rowContent := fmt.Sprintf("%s%s%s  %s%s", prefix, box, items[idx].TitleID, nameStr, strings.Repeat(" ", namePad))
					printBordered(rowContent)
				}
			}

			fmt.Printf("\r\033[2K╚%s╝\n", strings.Repeat("═", innerW))
			visibleEnd := min(total, scrollOffset+pageSize)
			fmt.Printf("\r\033[2K[Selected: %d / %d] (Scroll %d-%d of %d)\n",
				selectedCount, total, scrollOffset+1, visibleEnd, total)

			curPath := items[cursor].Path
			availStatus := max(10, termW-10)
			fmt.Printf("\r\033[2KTarget: %s\033[J", fitMiddle(curPath, availStatus))

			needRedraw = false
		}

		key := readKeyWithTimeout(40 * time.Millisecond)
		if key == KeyNone {
			continue
		}

		switch key {
		case KeyUp:
			if cursor > 0 {
				cursor--
				needRedraw = true
			}
		case KeyDown:
			if cursor < total-1 {
				cursor++
				needRedraw = true
			}
		case KeySpace:
			items[cursor].Selected = !items[cursor].Selected
			needRedraw = true
		case KeyToggleAll:
			selectedCount := 0
			for _, it := range items {
				if it.Selected {
					selectedCount++
				}
			}
			allChecked := selectedCount == total
			for i := range items {
				items[i].Selected = !allChecked
			}
			needRedraw = true
		case KeyEnter:
			var filtered []TitleItem
			for _, it := range items {
				if it.Selected {
					filtered = append(filtered, it)
				}
			}
			return filtered, true
		case KeyQuit:
			return nil, false
		}
	}
}

func readLineStdin(reader *bufio.Reader) string {
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line)
}

func askYesNo(reader *bufio.Reader, prompt string, defaultYes bool) bool {
	for {
		fmt.Print(prompt)
		ans := strings.ToLower(readLineStdin(reader))
		if ans == "" {
			return defaultYes
		}
		if ans == "y" || ans == "yes" {
			return true
		}
		if ans == "n" || ans == "no" {
			return false
		}
		fmt.Println("Invalid input. Please enter 'y' or 'n'.")
	}
}

func main() {
	cleanupVT := enableVirtualTerminal()
	defer cleanupVT()

	if len(os.Args) > 1 && os.Args[1] == "--generate-titles" {
		client := newHTTPClient()
		if _, err := cemumegadownloader.GenerateTitlesJSON(client, "titles.json"); err != nil {
			fmt.Fprintf(os.Stderr, "[Error] %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[Info] titles.json generated successfully.")
		return
	}

	reader := bufio.NewReader(os.Stdin)

	cemuDir, _ := detectCemuDirectories()
	detectedReg := detectCemuRegion(cemuDir)

	for {
		fmt.Println("Menu:")
		if detectedReg != "" {
			fmt.Printf(" [Cemu detected region: %s]\n", detectedReg)
		}
		fmt.Println("1. Download EUR titles")
		fmt.Println("2. Download USA titles")
		fmt.Println("3. Download JPN titles")
		fmt.Println("4. Exit")
		fmt.Print("Select an option: ")

		choice := readLineStdin(reader)
		switch choice {
		case "1":
			startRegionWorkflow("EUR", reader)
			return
		case "2":
			startRegionWorkflow("USA", reader)
			return
		case "3":
			startRegionWorkflow("JPN", reader)
			return
		case "4":
			return
		default:
			fmt.Println("Invalid choice. Try again.")
		}
	}
}

func startRegionWorkflow(region string, reader *bufio.Reader) {
	client := newHTTPClient()

	var targetBaseDir string
	cleanInstall := false

	detectedCemuDir, detectedMlc := detectCemuDirectories()
	detectedRegion := detectCemuRegion(detectedCemuDir)

	if detectedCemuDir != "" {
		absCemu, _ := filepath.Abs(detectedCemuDir)
		fmt.Printf("\n[Cemu Detected] Found Cemu directory at:\n  %s\n", absCemu)
		if detectedRegion != "" {
			fmt.Printf("[Cemu Detected] Active console region detected: %s\n", detectedRegion)
			if detectedRegion != region {
				fmt.Printf("[Notice] Note: Selected download region (%s) differs from detected Cemu region (%s).\n", region, detectedRegion)
			}
		}
		if detectedMlc != "" {
			absMlc, _ := filepath.Abs(detectedMlc)
			if err := checkDirectoryWritable(absMlc); err == nil {
				if askYesNo(reader, "Do you want to install titles directly into this Cemu directory? [Y/n]: ", true) {
					targetBaseDir = absMlc
					fmt.Printf("[Info] Destination set to: %s\n", targetBaseDir)
				}
			} else {
				fmt.Printf("[Warning] Missing write permissions for %s (%v).\n", absMlc, err)
			}
		}
	}

	if targetBaseDir == "" {
		if askYesNo(reader, "\nDo you want to specify a custom mlc01 path? [y/N]: ", false) {
			for {
				fmt.Print("Enter custom mlc01 path (or press Enter to cancel): ")
				inputPath := readLineStdin(reader)
				if inputPath == "" {
					break
				}
				cleaned := cleanPathInput(inputPath)
				if err := checkDirectoryWritable(cleaned); err == nil {
					absCustom, _ := filepath.Abs(cleaned)
					if cDir, mMlc := inspectCustomCemuPath(cleaned); cDir != "" {
						detectedCemuDir = cDir
						targetBaseDir = mMlc
						fmt.Printf("[Info] Recognized Cemu directory at: %s\n", cDir)
						fmt.Printf("[Info] Destination mlc01 set to: %s\n", targetBaseDir)
					} else {
						targetBaseDir = absCustom
						fmt.Printf("[Info] Custom destination set to: %s\n", targetBaseDir)
					}
					break
				} else {
					fmt.Printf("[Warning] Path '%s' is not writable or invalid: %v\n", cleaned, err)
					fmt.Println("Please try again.")
				}
			}
		}
	}

	if targetBaseDir == "" {
		targetBaseDir = "."
		fmt.Println("[Info] Using default local directory (./mlc01).")
		if err := checkDirectoryWritable("mlc01"); err != nil {
			fmt.Printf("[Critical Error] Unable to write to local mlc01 directory: %v\n", err)
			return
		}
	}

	mlcRoot := cleanPathInput(targetBaseDir)
	if !strings.HasSuffix(strings.ToLower(mlcRoot), "mlc01") {
		mlcRoot = filepath.Join(mlcRoot, "mlc01")
	}

	ensureStandardMLCDirectories(mlcRoot)

	if p := setupCafeLibs(mlcRoot, detectedCemuDir, targetBaseDir); p != "" {
		fmt.Printf("[Info] cafeLibs already populated from existing files at:\n  %s\n", p)
	}

	usrDir := filepath.Join(mlcRoot, "usr")
	if entries, err := os.ReadDir(usrDir); err == nil && len(entries) > 0 {
		fmt.Printf("\n[Notice] Existing 'usr' directory detected at:\n  %s\n", usrDir)
		fmt.Println("[Notice] It is strongly recommended to back up your 'usr' directory in case you have important game saves.")
	}

	sysTitleDir := filepath.Join(mlcRoot, "sys", "title")
	if entries, err := os.ReadDir(sysTitleDir); err == nil && len(entries) > 0 {
		fmt.Println("\nExisting system titles were detected in this mlc01 directory.")
		if askYesNo(reader, "Do you want to perform a clean installation (purge previous 'sys' title data)? [y/N]: ", false) {
			cleanInstall = true
			_ = os.RemoveAll(sysTitleDir)
			_ = os.MkdirAll(sysTitleDir, 0755)
			fmt.Println("[Info] Clean installation enabled: 'sys/title' has been purged. 'usr' was left untouched.")
		}
	}

	autoConfigureProfile := false
	if detectedCemuDir != "" {
		if askYesNo(reader, "\nDo you want to automatically configure Cemu to load cafeLibs for Wii U Menu? [Y/n]: ", true) {
			autoConfigureProfile = true
		}
	}

	fmt.Printf("\n[Info] Loading MLC title list for region %s...\n", region)
	availableItems, err := LoadTitlesForRegion(region, targetBaseDir, client)
	if err != nil {
		fmt.Printf("[Error] Failed to load titles: %v\n", err)
		return
	}

	selectedItems, ok := SelectTitlesInteractive(availableItems, region)
	if !ok || len(selectedItems) == 0 {
		fmt.Println("[Info] No titles selected or operation cancelled.")
		return
	}

	workers := defaultConcurrentTitles
	fmt.Printf("\nEnter number of concurrent downloads [1-10] (default: %d, recommended for most PCs): ", defaultConcurrentTitles)
	if input := readLineStdin(reader); input != "" {
		var w int
		if _, err := fmt.Sscanf(input, "%d", &w); err == nil && w >= 1 && w <= 16 {
			workers = w
		}
	}

	chunkWorkers := 4
	switch {
	case workers <= 2:
		chunkWorkers = 8
	case workers == 3:
		chunkWorkers = 6
	case workers == 4:
		chunkWorkers = 5
	case workers <= 7:
		chunkWorkers = 3
	default:
		chunkWorkers = 2
	}
	cemumegadownloader.SetMaxConcurrentDownloads(chunkWorkers)

	downloadBatch(selectedItems, region, client, cleanInstall, mlcRoot, detectedCemuDir, targetBaseDir, autoConfigureProfile, workers, reader)
}

func printPostDownloadSteps(region, cemuDir, cafeLibsPath string, profileConfigured bool) {
	if cemuDir != "" && profileConfigured {
		fmt.Println("\nSetup complete and ready for Cemu!")
		fmt.Printf("1. 'mlc01' and 'cafeLibs' are installed directly in your Cemu directory (%s).\n", cemuDir)
		fmt.Println("2. Wii U Menu game profile was automatically configured with 'loadSharedLibraries = true'.")
		fmt.Printf("3. In Cemu (Options -> General settings -> System), ensure Console Region is '%s' (or Auto).\n", region)
		fmt.Println("4. You can now launch Cemu and start the 'Wii U Menu' directly!")
	} else {
		fmt.Println("\nNext steps in Cemu:")
		if cemuDir == "" {
			fmt.Println("1. Copy both 'mlc01' and 'cafeLibs' into your Cemu directory (where Cemu executable is located).")
			fmt.Println("   Or in Cemu: Options -> General settings -> General -> set MLC Path to your 'mlc01' directory,")
			fmt.Println("   and place the 'cafeLibs' directory inside your Cemu directory next to Cemu executable.")
		} else {
			fmt.Printf("1. Ensure your Cemu MLC Path points to your 'mlc01' directory (%s/mlc01).\n", cemuDir)
		}
		if cafeLibsPath != "" {
			fmt.Printf("2. Ensure 'cafeLibs' directory is in your Cemu directory (%s).\n", cafeLibsPath)
		}
		fmt.Printf("3. In Cemu (Options -> General settings -> System), ensure Console Region matches '%s' (or Auto).\n", region)
		if !profileConfigured {
			fmt.Println("4. In Cemu's game list, right-click 'Wii U Menu' -> 'Edit Game Profile' -> General tab.")
			fmt.Println("   Ensure 'Load Shared Libraries' is checked.")
		}
		fmt.Println("5. Launch the Wii U Menu!")
	}
}

func downloadBatch(
	chosenTitles []TitleItem,
	region string,
	client *http.Client,
	cleanInstall bool,
	mlcRoot string,
	cemuDir string,
	targetBaseDir string,
	autoConfigureProfile bool,
	workers int,
	reader *bufio.Reader,
) {
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	totalTitles := len(chosenTitles)

	var (
		failedMu     sync.Mutex
		failedTitles []string
		verifiedCnt  int32
	)

	fmt.Printf("\n[Info] Scanning metadata for %d titles to calculate exact batch size...\n", totalTitles)

	var (
		totalBatchBytes int64
		scanGroup       errgroup.Group
	)
	scanGroup.SetLimit(16)

	for _, item := range chosenTitles {
		tid := item.TitleID
		scanGroup.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}
		sz, err := cemumegadownloader.GetTitleSize(tid, client)
			if err == nil && sz > 0 {
				atomic.AddInt64(&totalBatchBytes, sz)
			}
			return nil
		})
	}
	_ = scanGroup.Wait()

	progressReporter := NewProgressReporterCLI()
	progressReporter.SetBatchTotals(totalTitles, totalBatchBytes)

	if totalBatchBytes > 0 {
		fmt.Printf("[Info] Total size: %s (%d bytes)\n", formatBytes(totalBatchBytes), totalBatchBytes)
	}

	if workers < 1 {
		workers = defaultConcurrentTitles
	}

	fmt.Printf("\n[Info] Starting concurrent download of %d titles for region %s (%d titles in parallel)...\n\n",
		totalTitles, region, workers)

	var g errgroup.Group
	g.SetLimit(workers)

	for _, item := range chosenTitles {
		g.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}
			progressReporter.TitleStarted()
			progressReporter.LogLine("[Info] Downloading %s (%s)", item.TitleID, item.Name)

			err := cemumegadownloader.DownloadAndExtractTitle(
				ctx,
				item.TitleID,
				item.Path,
				client,
				cleanInstall,
				progressReporter.AddBytes,
			)
			if err != nil {
				progressReporter.TitleFinished(false)
				progressReporter.LogLine("[Error] Title %s failed: %v", item.TitleID, err)
				failedMu.Lock()
				failedTitles = append(failedTitles, item.TitleID)
				failedMu.Unlock()
				return nil
			}

			progressReporter.TitleFinished(true)
			doneNow := atomic.AddInt32(&verifiedCnt, 1)
			progressReporter.LogLine("[Info] [%d/%d] %s (%s) verified and extracted OK",
				doneNow, totalTitles, item.TitleID, item.Name)
			if item.TitleID == "000500101000400a" {
				if p := setupCafeLibs(mlcRoot, cemuDir, targetBaseDir); p != "" {
					progressReporter.LogLine("[Info] cafeLibs successfully populated at: %s", p)
				}
			}
			return nil
		})
	}

	_ = g.Wait()
	progressReporter.ClearLine()

	fmt.Printf("\n[Info] Running final completeness and integrity check on all %d titles...\n", totalTitles)
	var finalMissingOrCorrupt []string
	for idx, item := range chosenTitles {
		if err := cemumegadownloader.VerifyTitle(item.TitleID, item.Path); err != nil {
			fmt.Printf("[Error] [%d/%d] Final check FAILED for %s: %v\n", idx+1, totalTitles, item.TitleID, err)
			finalMissingOrCorrupt = append(finalMissingOrCorrupt, item.TitleID)
		}
	}

	fmt.Printf("\n[Info] Final Summary for %s: %d/%d titles present (extracted to mlc01 and verified)\n",
		region, totalTitles-len(finalMissingOrCorrupt), totalTitles)

	cafeLibsPath := setupCafeLibs(mlcRoot, cemuDir, targetBaseDir)
	if cafeLibsPath != "" {
		fmt.Printf("\n[Info] cafeLibs successfully configured at:\n  %s\n", cafeLibsPath)
	}

	profileConfigured := false
	if autoConfigureProfile && cemuDir != "" {
		if configureCemuGameProfile(cemuDir, region, reader) {
			profileConfigured = true
			fmt.Println("[Info] Cemu game profile configured with 'loadSharedLibraries = true'.")
		}
	}

	if len(finalMissingOrCorrupt) == 0 && len(failedTitles) == 0 {
		fmt.Println("[Info] All done! Every single title is complete, verified, and ready for Cemu.")
		printPostDownloadSteps(region, cemuDir, cafeLibsPath, profileConfigured)
		fmt.Println("\nPress Enter to exit...")
		_, _ = reader.ReadBytes('\n')
		os.Exit(0)
	} else {
		fmt.Printf("[Error] Titles failing verification: %v\n", finalMissingOrCorrupt)
		fmt.Println("[Error] Some titles were not able to be downloaded or verified.")
		printPostDownloadSteps(region, cemuDir, cafeLibsPath, profileConfigured)
		fmt.Println("\nPress Enter to exit...")
		_, _ = reader.ReadBytes('\n')
		os.Exit(1)
	}
}
