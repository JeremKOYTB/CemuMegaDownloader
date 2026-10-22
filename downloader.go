package cemumegadownloader

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed titles.json
var EmbeddedTitlesJSON []byte

var WiiUCommonKey = []byte{
	0xd7, 0xb0, 0x04, 0x02, 0x65, 0x9b, 0xa2, 0xab,
	0xd2, 0xcb, 0x0d, 0xb2, 0x7f, 0xa2, 0xb6, 0x56,
}

const (
	WiiUCommonKeySHA1      = "6a0b87fc98b306ae3366f0e0a88d0b06a2813313"
	osv10TID               = "000500101000400a"
	h3TotalBlock           = 0x10000
	h3HeaderSize           = 0x400
	h3BatchSize            = h3TotalBlock * 32
	linearChunkSize        = 4 * 1024 * 1024
	maxHTTPMetadataBytes   = 16 * 1024 * 1024
	maxRetries             = 5
	retryDelay             = 3 * time.Second
	maxConcurrentDownloads = 4
	RemoteMLCTitlesURL     = "https://raw.githubusercontent.com/Xpl0itU/MLCRestorerDownloader/refs/heads/main/titles.json"
	wiiuBrewDBURL          = "https://wiiubrew.org/wiki/Title_database"
)

var (
	defaultCertCache []byte
	certCacheLock    sync.Mutex

	reHTMLRow      = regexp.MustCompile(`(?is)<tr[^>]*>(.*?)</tr>`)
	reHTMLCell     = regexp.MustCompile(`(?is)<td[^>]*>(.*?)</td>`)
	reHTMLTag      = regexp.MustCompile(`(?is)<[^>]+>`)
	reHTMLBr       = regexp.MustCompile(`(?is)<br\s*/?>`)
	reBrewTID      = regexp.MustCompile(`(0005[0-9a-fA-F]{4}-?[0-9a-fA-F]{8})`)
	reReqMenuParen = regexp.MustCompile(`(?i)\s*\(Required for (?:launch )?Wii U Menu\)`)
)

type TitleDefinition struct {
	TitleID string `json:"titleID"`
	Name    string `json:"name"`
}

type TitlesCatalog struct {
	EUR []TitleDefinition `json:"EUR"`
	USA []TitleDefinition `json:"USA"`
	JPN []TitleDefinition `json:"JPN"`
	All []TitleDefinition `json:"All,omitempty"`
}

type Content struct {
	ID    uint32
	Index uint16
	Type  uint16
	Size  uint64
	Hash  []byte
}

type TMD struct {
	TitleID      uint64
	Version      byte
	TitleVersion uint16
	ContentCount uint16
	Contents     []Content
	Certificate1 []byte
	Certificate2 []byte
}

func IsRawTitle(titleID string) bool {
	norm := NormalizeTID(titleID)
	rawTIDs := map[string]bool{
		"0005001010000100": true,
		"0005001010000400": true,
	}
	return rawTIDs[norm]
}

func NormalizeTID(titleID string) string {
	r := strings.NewReplacer("-", "", "/", "", "\\", "")
	return strings.ToLower(strings.TrimSpace(r.Replace(titleID)))
}

func CleanTitleName(name string) string {
	cleaned := reReqMenuParen.ReplaceAllString(name, "")
	return strings.TrimSpace(cleaned)
}

func isSafeSingleName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\") {
		return false
	}
	return filepath.IsLocal(name)
}

func isPathInsideBase(baseDir, targetPath string) bool {
	rel, err := filepath.Rel(baseDir, targetPath)
	if err != nil {
		return false
	}
	return filepath.IsLocal(rel)
}

func fetchHTTPText(ctx context.Context, client *http.Client, urlStr string, timeout time.Duration) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, urlStr, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	b, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPMetadataBytes))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func loadEmbeddedNameFallback() map[string]string {
	names := make(map[string]string)
	var cat TitlesCatalog
	if json.Unmarshal(EmbeddedTitlesJSON, &cat) == nil {
		for _, list := range [][]TitleDefinition{cat.EUR, cat.USA, cat.JPN, cat.All} {
			for _, item := range list {
				norm := NormalizeTID(item.TitleID)
				cleanName := CleanTitleName(item.Name)
				if norm != "" && cleanName != "" && cleanName != norm {
					names[norm] = cleanName
				}
			}
		}
	}
	return names
}

func scrapeWiiUBrewNames(ctx context.Context, client *http.Client) map[string]string {
	names := make(map[string]string)
	body, err := fetchHTTPText(ctx, client, wiiuBrewDBURL, 15*time.Second)
	if err != nil {
		return names
	}

	rows := reHTMLRow.FindAllStringSubmatch(body, -1)
	for _, r := range rows {
		if len(r) < 2 {
			continue
		}
		cells := reHTMLCell.FindAllStringSubmatch(r[1], -1)
		if len(cells) < 2 {
			continue
		}

		c0 := strings.TrimSpace(reHTMLTag.ReplaceAllString(cells[0][1], ""))
		tidMatch := reBrewTID.FindStringSubmatch(c0)
		if len(tidMatch) < 2 {
			continue
		}
		tid := NormalizeTID(tidMatch[1])

		c1 := reHTMLBr.ReplaceAllString(cells[1][1], "\n")
		c1 = reHTMLTag.ReplaceAllString(c1, "")
		c1 = strings.TrimSpace(html.UnescapeString(c1))
		desc := strings.TrimSpace(strings.Split(c1, "\n")[0])
		cleanDesc := strings.Trim(desc, "'\"`")

		var resolvedName string
		if strings.HasPrefix(desc, "'") && strings.HasSuffix(desc, "'") && len(cells) > 2 {
			note := reHTMLBr.ReplaceAllString(cells[2][1], "\n")
			note = reHTMLTag.ReplaceAllString(note, "")
			note = strings.TrimSpace(html.UnescapeString(note))
			noteLine := strings.TrimSpace(strings.Split(note, "\n")[0])

			if len(noteLine) > 0 && len(noteLine) <= 45 &&
				!strings.HasPrefix(noteLine, "Contains ") &&
				!strings.HasPrefix(noteLine, "v") &&
				!strings.HasSuffix(noteLine, ".") &&
				!strings.EqualFold(noteLine, "TODO") &&
				!strings.EqualFold(noteLine, "N/A") {
				resolvedName = noteLine
			} else {
				resolvedName = cleanDesc
			}
		} else if cleanDesc != "" && !strings.EqualFold(cleanDesc, "N/A") {
			resolvedName = cleanDesc
		}

		resolvedName = CleanTitleName(resolvedName)
		if resolvedName != "" {
			names[tid] = resolvedName
		}
	}
	return names
}

func GenerateTitlesJSON(client *http.Client, dstPath string) ([]byte, error) {
	ctx := context.Background()
	nameMap := loadEmbeddedNameFallback()

	var wg sync.WaitGroup
	var wMap map[string]string
	var rawMLC string

	wg.Add(2)
	go func() {
		defer wg.Done()
		rawMLC, _ = fetchHTTPText(ctx, client, RemoteMLCTitlesURL, 12*time.Second)
	}()
	go func() {
		defer wg.Done()
		wMap = scrapeWiiUBrewNames(ctx, client)
	}()
	wg.Wait()

	for k, v := range wMap {
		if v != "" {
			nameMap[k] = CleanTitleName(v)
		}
	}

	var embeddedCat TitlesCatalog
	_ = json.Unmarshal(EmbeddedTitlesJSON, &embeddedCat)

	var parsedMLC struct {
		MLC map[string][]string `json:"MLC"`
	}

	mandatoryApplets := map[string][]string{
		"EUR": {"0005003010010209", "0005003010011209", "00050030100112ff", "0005003010017209"},
		"USA": {"0005003010010109", "0005003010011109", "00050030100111ff", "0005003010017109"},
		"JPN": {"0005003010010009", "0005003010011009", "00050030100110ff", "0005003010017009"},
	}

	if rawMLC != "" && json.Unmarshal([]byte(rawMLC), &parsedMLC) == nil && len(parsedMLC.MLC) > 0 {
		catalog := TitlesCatalog{
			EUR: []TitleDefinition{},
			USA: []TitleDefinition{},
			JPN: []TitleDefinition{},
		}

		buildRegion := func(reg string, embeddedFallback []TitleDefinition) []TitleDefinition {
			seen := make(map[string]struct{})
			var out []TitleDefinition

			addTID := func(rawTID string) {
				norm := NormalizeTID(rawTID)
				if norm == "" || norm == "dummy" {
					return
				}
				if _, exists := seen[norm]; !exists {
					seen[norm] = struct{}{}
					titleName := CleanTitleName(nameMap[norm])
					if titleName == "" {
						titleName = norm
					}
					out = append(out, TitleDefinition{
						TitleID: norm,
						Name:    titleName,
					})
				}
			}

			for _, item := range embeddedFallback {
				addTID(item.TitleID)
			}
			for _, tid := range parsedMLC.MLC["All"] {
				addTID(tid)
			}
			for _, tid := range parsedMLC.MLC[reg] {
				addTID(tid)
			}
			for _, tid := range mandatoryApplets[reg] {
				addTID(tid)
			}
			return out
		}

		catalog.EUR = buildRegion("EUR", embeddedCat.EUR)
		catalog.USA = buildRegion("USA", embeddedCat.USA)
		catalog.JPN = buildRegion("JPN", embeddedCat.JPN)

		outBytes, err := json.MarshalIndent(catalog, "", "  ")
		if err == nil {
			if dstPath != "" {
				_ = os.WriteFile(dstPath, outBytes, 0644)
			}
			return outBytes, nil
		}
	}

	if len(EmbeddedTitlesJSON) > 0 {
		var cleanCat TitlesCatalog
		if json.Unmarshal(EmbeddedTitlesJSON, &cleanCat) == nil {
			for i := range cleanCat.EUR {
				cleanCat.EUR[i].Name = CleanTitleName(cleanCat.EUR[i].Name)
			}
			for i := range cleanCat.USA {
				cleanCat.USA[i].Name = CleanTitleName(cleanCat.USA[i].Name)
			}
			for i := range cleanCat.JPN {
				cleanCat.JPN[i].Name = CleanTitleName(cleanCat.JPN[i].Name)
			}
			cleanCat.All = nil
			if outBytes, err := json.MarshalIndent(cleanCat, "", "  "); err == nil {
				if dstPath != "" {
					_ = os.WriteFile(dstPath, outBytes, 0644)
				}
				return outBytes, nil
			}
		}
		if dstPath != "" {
			_ = os.WriteFile(dstPath, EmbeddedTitlesJSON, 0644)
		}
		return EmbeddedTitlesJSON, nil
	}

	return nil, errors.New("failed to generate titles.json")
}

func ParseTMD(data []byte) (*TMD, error) {
	if len(data) < 0x200 {
		return nil, fmt.Errorf("TMD file too small: %d bytes", len(data))
	}

	tmd := &TMD{}
	reader := bytes.NewReader(data)

	_, _ = reader.Seek(0x180, io.SeekStart)
	if err := binary.Read(reader, binary.BigEndian, &tmd.Version); err != nil {
		return nil, err
	}

	switch tmd.Version {
	case 0x00:
		_, _ = reader.Seek(0x18C, io.SeekStart)
		if err := binary.Read(reader, binary.BigEndian, &tmd.TitleID); err != nil {
			return nil, err
		}
		_, _ = reader.Seek(0x1DC, io.SeekStart)
		if err := binary.Read(reader, binary.BigEndian, &tmd.TitleVersion); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &tmd.ContentCount); err != nil {
			return nil, err
		}

		certOffset := uint64(0x1E4) + (uint64(tmd.ContentCount) * 0x24)
		if uint64(len(data)) < certOffset {
			return nil, fmt.Errorf("TMD v0 too small for %d contents (needs %d bytes, got %d)", tmd.ContentCount, certOffset, len(data))
		}

		tmd.Contents = make([]Content, tmd.ContentCount)
		for i := 0; i < int(tmd.ContentCount); i++ {
			offset := int64(0x1E4 + (0x24 * i))
			_, _ = reader.Seek(offset, io.SeekStart)
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[i].ID); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[i].Index); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[i].Type); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[i].Size); err != nil {
				return nil, err
			}
			tmd.Contents[i].Hash = make([]byte, 0x14)
			if _, err := io.ReadFull(reader, tmd.Contents[i].Hash); err != nil {
				return nil, err
			}
		}

		_, _ = reader.Seek(int64(certOffset), io.SeekStart)
		if uint64(len(data)) >= certOffset+0x700 {
			tmd.Certificate1 = make([]byte, 0x400)
			if _, err := io.ReadFull(reader, tmd.Certificate1); err != nil {
				return nil, err
			}
			tmd.Certificate2 = make([]byte, 0x300)
			if _, err := io.ReadFull(reader, tmd.Certificate2); err != nil {
				return nil, err
			}
		}

	case 0x01:
		_, _ = reader.Seek(0x18C, io.SeekStart)
		if err := binary.Read(reader, binary.BigEndian, &tmd.TitleID); err != nil {
			return nil, err
		}
		_, _ = reader.Seek(0x1DC, io.SeekStart)
		if err := binary.Read(reader, binary.BigEndian, &tmd.TitleVersion); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &tmd.ContentCount); err != nil {
			return nil, err
		}

		certOffset := uint64(0xB04) + (uint64(tmd.ContentCount) * 0x30)
		if uint64(len(data)) < certOffset {
			return nil, fmt.Errorf("TMD v1 too small for %d contents (needs %d bytes, got %d)", tmd.ContentCount, certOffset, len(data))
		}

		tmd.Contents = make([]Content, tmd.ContentCount)
		for c := 0; c < int(tmd.ContentCount); c++ {
			offset := int64(0xB04 + (0x30 * c))
			_, _ = reader.Seek(offset, io.SeekStart)
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[c].ID); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[c].Index); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[c].Type); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tmd.Contents[c].Size); err != nil {
				return nil, err
			}
			tmd.Contents[c].Hash = make([]byte, 0x14)
			if _, err := io.ReadFull(reader, tmd.Contents[c].Hash); err != nil {
				return nil, err
			}
		}

		_, _ = reader.Seek(int64(certOffset), io.SeekStart)
		if uint64(len(data)) >= certOffset+0x700 {
			tmd.Certificate1 = make([]byte, 0x400)
			if _, err := io.ReadFull(reader, tmd.Certificate1); err != nil {
				return nil, err
			}
			tmd.Certificate2 = make([]byte, 0x300)
			if _, err := io.ReadFull(reader, tmd.Certificate2); err != nil {
				return nil, err
			}
		}

	default:
		return nil, fmt.Errorf("unknown TMD version: %d", tmd.Version)
	}

	return tmd, nil
}

func getTrimmedTMDBytes(rawTMD []byte, tmd *TMD) []byte {
	var certOffset uint64
	if tmd.Version == 0x00 {
		certOffset = uint64(0x1E4) + (uint64(tmd.ContentCount) * 0x24)
	} else {
		certOffset = uint64(0xB04) + (uint64(tmd.ContentCount) * 0x30)
	}
	if uint64(len(rawTMD)) >= certOffset {
		return rawTMD[:certOffset]
	}
	return rawTMD
}

func getDefaultRootCert(ctx context.Context, client *http.Client) ([]byte, error) {
	certCacheLock.Lock()
	defer certCacheLock.Unlock()

	if len(defaultCertCache) == 0x300 {
		return defaultCertCache, nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://ccs.cdn.c.shop.nintendowifi.net/ccs/download/%s/cetk", osv10TID)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "WiiUDownloader")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch root certificate: HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPMetadataBytes))
	if err != nil {
		return nil, err
	}
	if len(data) < 0x650 {
		return nil, fmt.Errorf("invalid cetk size: %d", len(data))
	}

	defaultCertCache = make([]byte, 0x300)
	copy(defaultCertCache, data[0x350:0x650])
	return defaultCertCache, nil
}

func generateCert(ctx context.Context, tmd *TMD, certPath string, client *http.Client) error {
	caCert, err := getDefaultRootCert(ctx, client)
	if err != nil {
		return err
	}

	fullCert := make([]byte, 0, len(tmd.Certificate1)+len(tmd.Certificate2)+len(caCert))
	fullCert = append(fullCert, tmd.Certificate1...)
	fullCert = append(fullCert, tmd.Certificate2...)
	fullCert = append(fullCert, caCert...)

	return os.WriteFile(certPath, fullCert, 0644)
}

func ReadTMDAndTitleKey(tmdPath, tikPath string, expectedTID string) (*TMD, []byte, error) {
	rawTMD, err := os.ReadFile(tmdPath)
	if err != nil {
		return nil, nil, err
	}
	if len(rawTMD) < 0x200 {
		return nil, nil, fmt.Errorf("invalid TMD size: %d", len(rawTMD))
	}

	tmd, err := ParseTMD(rawTMD)
	if err != nil {
		return nil, nil, err
	}
	if tmd.ContentCount == 0 || len(tmd.Contents) != int(tmd.ContentCount) {
		return nil, nil, errors.New("invalid TMD content count")
	}

	if expectedTID != "" {
		normExpected := NormalizeTID(expectedTID)
		expectedInt, err := strconv.ParseUint(normExpected, 16, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid expected title ID %s: %w", expectedTID, err)
		}
		if tmd.TitleID != expectedInt {
			return nil, nil, fmt.Errorf("TMD title ID mismatch: got %016x, expected %016x", tmd.TitleID, expectedInt)
		}
	}

	rawTik, err := os.ReadFile(tikPath)
	if err != nil {
		return nil, nil, err
	}
	if len(rawTik) < 0x1D0 {
		return nil, nil, fmt.Errorf("invalid ticket size: %d", len(rawTik))
	}
	if len(rawTik) >= 0x1E4 {
		tikTID := binary.BigEndian.Uint64(rawTik[0x1DC:0x1E4])
		if tikTID != tmd.TitleID {
			return nil, nil, fmt.Errorf("manifest mismatch between TMD (%016x) and ticket (%016x)", tmd.TitleID, tikTID)
		}
	}

	commonBlock, err := aes.NewCipher(WiiUCommonKey)
	if err != nil {
		return nil, nil, err
	}

	var ivTKey [16]byte
	copy(ivTKey[:8], rawTMD[0x18C:0x194])

	titleKey := make([]byte, 16)
	cipher.NewCBCDecrypter(commonBlock, ivTKey[:]).CryptBlocks(titleKey, rawTik[0x1BF:0x1CF])

	return tmd, titleKey, nil
}

func resolveContentFile(workDir string, cid uint32, ext string) (string, error) {
	candidates := []string{
		filepath.Join(workDir, fmt.Sprintf("%08X%s", cid, ext)),
		filepath.Join(workDir, fmt.Sprintf("%08x%s", cid, ext)),
	}
	for _, p := range candidates {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, nil
		}
	}
	return candidates[0], fmt.Errorf("missing file %s", filepath.Base(candidates[0]))
}

func verifySingleContent(workDir string, c Content, titleBlock cipher.Block) error {
	appPath, err := resolveContentFile(workDir, c.ID, ".app")
	if err != nil {
		return err
	}
	appName := filepath.Base(appPath)

	if len(c.Hash) < 20 {
		return fmt.Errorf("invalid content hash length in TMD for %s", appName)
	}
	expectedHash := c.Hash[:20]

	var baseIV [16]byte
	binary.BigEndian.PutUint16(baseIV[0:2], c.Index)

	if (c.Type & 0x02) != 0 {
		h3Path, err := resolveContentFile(workDir, c.ID, ".h3")
		if err != nil {
			return err
		}
		h3Name := filepath.Base(h3Path)

		h3Raw, err := os.ReadFile(h3Path)
		if err != nil {
			return err
		}

		h3Sum := sha1.Sum(h3Raw)
		if !bytes.Equal(h3Sum[:], expectedHash) {
			return fmt.Errorf("H3 SHA-1 mismatch for %s", h3Name)
		}

		fApp, err := os.Open(appPath)
		if err != nil {
			return err
		}
		defer fApp.Close()

		idxHi := byte((c.Index >> 8) & 0xFF)
		idxLo := byte(c.Index & 0xFF)
		chunkIdx := 0

		buf := make([]byte, h3BatchSize)
		hashes := make([]byte, h3HeaderSize)
		decPayload := make([]byte, h3TotalBlock-h3HeaderSize)
		var cIV [16]byte
		var expectedTrueHash [20]byte

		for {
			nRead, readErr := io.ReadFull(fApp, buf)
			if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				return readErr
			}
			if nRead == 0 {
				break
			}

			pos := 0
			for pos < nRead {
				blockEnd := pos + h3TotalBlock
				if blockEnd > nRead {
					blockEnd = nRead
				}
				blockLen := blockEnd - pos
				if blockLen <= h3HeaderSize {
					return fmt.Errorf("short H3 block in %s at chunk %d", appName, chunkIdx)
				}

				cipher.NewCBCDecrypter(titleBlock, baseIV[:]).CryptBlocks(hashes, buf[pos:pos+h3HeaderSize])

				blockNum := chunkIdx & 0xF
				h0Off := blockNum * 20
				copy(expectedTrueHash[:], hashes[h0Off:h0Off+20])
				copy(cIV[:], expectedTrueHash[:16])

				if blockNum == 0 {
					cIV[0] ^= idxHi
					cIV[1] ^= idxLo
					expectedTrueHash[0] ^= idxHi
					expectedTrueHash[1] ^= idxLo
				}

				payloadLen := blockLen - h3HeaderSize
				encSlice := buf[pos+h3HeaderSize : blockEnd]
				rem := payloadLen & 0xF

				bodyDec := cipher.NewCBCDecrypter(titleBlock, cIV[:])
				var validDec []byte

				if rem != 0 {
					paddedLen := payloadLen + (16 - rem)
					paddedEnc := make([]byte, paddedLen)
					copy(paddedEnc, encSlice)
					paddedDec := make([]byte, paddedLen)
					bodyDec.CryptBlocks(paddedDec, paddedEnc)
					validDec = paddedDec[:payloadLen]
				} else {
					bodyDec.CryptBlocks(decPayload[:payloadLen], encSlice)
					validDec = decPayload[:payloadLen]
				}

				payloadSum := sha1.Sum(validDec)
				if payloadSum != expectedTrueHash {
					return fmt.Errorf("H0 hash mismatch in %s at chunk %d", appName, chunkIdx)
				}

				chunkIdx++
				pos += h3TotalBlock
			}

			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
		}

		return nil
	}

	stat, err := os.Stat(appPath)
	if err != nil {
		return err
	}
	actualSize := uint64(stat.Size())
	alignedSize := (c.Size + 15) & ^uint64(15)
	if actualSize != c.Size && actualSize != alignedSize {
		return fmt.Errorf("size mismatch for %s: got %d, expected %d", appName, actualSize, c.Size)
	}

	fApp, err := os.Open(appPath)
	if err != nil {
		return err
	}
	defer fApp.Close()

	decMode := cipher.NewCBCDecrypter(titleBlock, baseIV[:])
	hasher := sha1.New()

	var processedSize uint64
	buf := make([]byte, linearChunkSize)
	decBuf := make([]byte, linearChunkSize)

	for processedSize < c.Size {
		nRead, readErr := fApp.Read(buf)
		if nRead > 0 {
			rem := nRead & 0xF
			var chunkLen uint64
			var decSlice []byte

			if rem != 0 {
				paddedLen := nRead + (16 - rem)
				paddedEnc := make([]byte, paddedLen)
				copy(paddedEnc, buf[:nRead])
				paddedDec := make([]byte, paddedLen)
				decMode.CryptBlocks(paddedDec, paddedEnc)
				decSlice = paddedDec
				chunkLen = uint64(paddedLen)
			} else {
				decMode.CryptBlocks(decBuf[:nRead], buf[:nRead])
				decSlice = decBuf[:nRead]
				chunkLen = uint64(nRead)
			}

			if processedSize+uint64(len(decSlice)) >= c.Size {
				validLen := c.Size - processedSize
				hasher.Write(decSlice[:validLen])
				processedSize += validLen
				break
			}

			hasher.Write(decSlice)
			processedSize += chunkLen
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}

	if processedSize < c.Size {
		return fmt.Errorf("unexpected EOF in %s (%d/%d bytes)", appName, processedSize, c.Size)
	}

	computed := hasher.Sum(nil)
	if !bytes.Equal(computed, expectedHash) {
		return fmt.Errorf("linear SHA-1 mismatch for %s", appName)
	}

	return nil
}

func VerifyTitle(titleID, outputDirectory string) error {
	outputDir := strings.TrimRight(outputDirectory, "/\\")
	normTID := NormalizeTID(titleID)

	if IsRawTitle(normTID) {
		tmdPath := filepath.Join(outputDir, "title.tmd")
		tikPath := filepath.Join(outputDir, "title.tik")
		if _, err := os.Stat(tmdPath); err == nil {
			if _, errTik := os.Stat(tikPath); errTik == nil {
				tmd, titleKey, err := ReadTMDAndTitleKey(tmdPath, tikPath, titleID)
				if err != nil {
					return err
				}
				titleBlock, err := aes.NewCipher(titleKey)
				if err != nil {
					return err
				}
				for _, c := range tmd.Contents {
					if err := verifySingleContent(outputDir, c, titleBlock); err != nil {
						return err
					}
				}
				if normTID == "0005001010000400" {
					decP := filepath.Join(outputDir, "00000000.dec")
					if st, errDec := os.Stat(decP); errDec != nil || st.Size() == 0 {
						return fmt.Errorf("missing or empty 00000000.dec for %s", titleID)
					}
				}
				return nil
			}
		}

		switch normTID {
		case "0005001010000100":
			decP := filepath.Join(outputDir, "00000009.dec")
			if st, err := os.Stat(decP); err != nil || st.Size() == 0 {
				return fmt.Errorf("missing or empty 00000009.dec for %s", titleID)
			}
			return nil
		case "0005001010000400":
			decP := filepath.Join(outputDir, "00000000.dec")
			if st, err := os.Stat(decP); err != nil || st.Size() == 0 {
				return fmt.Errorf("missing or empty 00000000.dec for %s", titleID)
			}
			return nil
		}
	}

	tmdPath := filepath.Join(outputDir, "title.tmd")
	tikPath := filepath.Join(outputDir, "title.tik")

	if _, err1 := os.Stat(tmdPath); err1 == nil {
		if _, err2 := os.Stat(tikPath); err2 == nil {
			tmd, titleKey, err := ReadTMDAndTitleKey(tmdPath, tikPath, titleID)
			if err != nil {
				return fmt.Errorf("integrity check failed for %s: %w", titleID, err)
			}

			titleBlock, err := aes.NewCipher(titleKey)
			if err != nil {
				return err
			}

			hasRawApps := true
			for _, c := range tmd.Contents {
				if _, err := resolveContentFile(outputDir, c.ID, ".app"); err != nil {
					hasRawApps = false
					break
				}
			}

			if hasRawApps {
				for _, c := range tmd.Contents {
					if err := verifySingleContent(outputDir, c, titleBlock); err != nil {
						return fmt.Errorf("title %s content %08X verification failed: %w", titleID, c.ID, err)
					}
				}
				return nil
			}
		}
	}

	for _, sub := range []string{"code", "content", "meta"} {
		subDir := filepath.Join(outputDir, sub)
		if entries, err := os.ReadDir(subDir); err == nil && len(entries) > 0 {
			return nil
		}
	}

	return fmt.Errorf("verification failed for %s: missing valid extracted files or manifests", titleID)
}

func extractFileStandard(srcApp, dstPath string, titleBlock cipher.Block, contentIdx uint16, offset, length uint64) error {
	_ = os.Remove(dstPath)
	if length == 0 {
		return os.WriteFile(dstPath, []byte{}, 0644)
	}

	fIn, err := os.Open(srcApp)
	if err != nil {
		return err
	}
	defer fIn.Close()

	var baseIV [16]byte
	binary.BigEndian.PutUint16(baseIV[0:2], contentIdx)

	alignedOffset := offset & ^uint64(15)
	skipBytes := offset - alignedOffset

	var iv [16]byte
	if alignedOffset > 0 {
		if _, err := fIn.Seek(int64(alignedOffset-16), io.SeekStart); err != nil {
			return err
		}
		if _, err := io.ReadFull(fIn, iv[:]); err != nil {
			return err
		}
	} else {
		copy(iv[:], baseIV[:])
	}

	if _, err := fIn.Seek(int64(alignedOffset), io.SeekStart); err != nil {
		return err
	}

	fOut, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer fOut.Close()

	dec := cipher.NewCBCDecrypter(titleBlock, iv[:])
	remaining := length
	firstChunk := true

	totalAlignedNeed := (remaining + skipBytes + 15) & ^uint64(15)
	bufSize := min(uint64(linearChunkSize), max(uint64(4096), totalAlignedNeed))
	encBuf := make([]byte, bufSize)
	decBuf := make([]byte, bufSize)

	for remaining > 0 {
		toRead := bufSize
		need := (remaining + skipBytes + 15) & ^uint64(15)
		if need < toRead {
			toRead = need
		}

		nRead, readErr := io.ReadFull(fIn, encBuf[:toRead])
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if nRead == 0 {
			return fmt.Errorf("unexpected EOF while extracting %s (%d bytes remaining)", filepath.Base(dstPath), remaining)
		}

		rem := nRead & 0xF
		var chunkSlice []byte
		if rem != 0 {
			paddedLen := nRead + (16 - rem)
			padded := make([]byte, paddedLen)
			copy(padded, encBuf[:nRead])
			pDec := make([]byte, paddedLen)
			dec.CryptBlocks(pDec, padded)
			chunkSlice = pDec[:nRead]
		} else {
			dec.CryptBlocks(decBuf[:nRead], encBuf[:nRead])
			chunkSlice = decBuf[:nRead]
		}

		if firstChunk {
			if uint64(len(chunkSlice)) > skipBytes {
				chunkSlice = chunkSlice[skipBytes:]
			} else {
				chunkSlice = nil
			}
			firstChunk = false
		}

		writeLen := min(uint64(len(chunkSlice)), remaining)
		if writeLen > 0 {
			if _, err := fOut.Write(chunkSlice[:writeLen]); err != nil {
				return err
			}
			remaining -= writeLen
		}

		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			if remaining > 0 {
				return fmt.Errorf("truncated content while extracting %s (%d bytes missing)", filepath.Base(dstPath), remaining)
			}
			break
		}
	}

	return nil
}

func extractFileHashed(srcApp, dstPath string, titleBlock cipher.Block, contentIdx uint16, offset, length uint64) error {
	_ = os.Remove(dstPath)
	if length == 0 {
		return os.WriteFile(dstPath, []byte{}, 0644)
	}

	fIn, err := os.Open(srcApp)
	if err != nil {
		return err
	}
	defer fIn.Close()

	fOut, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer fOut.Close()

	const dataBlock = 0xFC00
	const totalBlock = 0x10000

	startChunkIdx := offset / dataBlock
	soffset := offset % dataBlock

	var baseIV [16]byte
	binary.BigEndian.PutUint16(baseIV[0:2], contentIdx)
	idxHi := byte((contentIdx >> 8) & 0xFF)
	idxLo := byte(contentIdx & 0xFF)

	remaining := length
	currentChunkIdx := startChunkIdx

	blockBuf := make([]byte, totalBlock)
	hashes := make([]byte, h3HeaderSize)
	decPayload := make([]byte, dataBlock)
	var cIV [16]byte

	for remaining > 0 {
		targetPos := int64(currentChunkIdx) * int64(totalBlock)
		if _, err := fIn.Seek(targetPos, io.SeekStart); err != nil {
			return err
		}

		nRead, readErr := io.ReadFull(fIn, blockBuf)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if nRead <= h3HeaderSize {
			return fmt.Errorf("unexpected EOF in hashed content while extracting %s", filepath.Base(dstPath))
		}

		cipher.NewCBCDecrypter(titleBlock, baseIV[:]).CryptBlocks(hashes, blockBuf[:h3HeaderSize])

		blockNum := currentChunkIdx & 0xF
		h0Off := blockNum * 20
		copy(cIV[:], hashes[h0Off:h0Off+16])

		if blockNum == 0 {
			cIV[0] ^= idxHi
			cIV[1] ^= idxLo
		}

		payloadLen := nRead - h3HeaderSize
		encSlice := blockBuf[h3HeaderSize:nRead]
		rem := payloadLen & 0xF

		bodyDec := cipher.NewCBCDecrypter(titleBlock, cIV[:])
		var validDec []byte

		if rem != 0 {
			paddedLen := payloadLen + (16 - rem)
			padded := make([]byte, paddedLen)
			copy(padded, encSlice)
			pDec := make([]byte, paddedLen)
			bodyDec.CryptBlocks(pDec, padded)
			validDec = pDec[:payloadLen]
		} else {
			bodyDec.CryptBlocks(decPayload[:payloadLen], encSlice)
			validDec = decPayload[:payloadLen]
		}

		if soffset >= uint64(len(validDec)) {
			return fmt.Errorf("invalid FST offset in hashed content for %s", filepath.Base(dstPath))
		}

		avail := uint64(len(validDec)) - soffset
		writeLen := min(remaining, avail)

		if _, err := fOut.Write(validDec[soffset : soffset+writeLen]); err != nil {
			return err
		}
		remaining -= writeLen

		soffset = 0
		currentChunkIdx++

		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			if remaining > 0 {
				return fmt.Errorf("truncated hashed content while extracting %s (%d bytes missing)", filepath.Base(dstPath), remaining)
			}
			break
		}
	}

	return nil
}

func ExtractRawTitle(workDir string, tmd *TMD, titleBlock cipher.Block) error {
	for _, c := range tmd.Contents {
		srcApp, err := resolveContentFile(workDir, c.ID, ".app")
		if err != nil {
			return err
		}

		decName := fmt.Sprintf("%08x.dec", c.ID)
		dstPath := filepath.Join(workDir, decName)

		if err := extractFileStandard(srcApp, dstPath, titleBlock, c.Index, 0, c.Size); err != nil {
			return fmt.Errorf("failed to decrypt raw content %s: %w", decName, err)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func shouldZeroBootLogo(normTID string) bool {
	switch normTID {
	case "0005001010066000", "0005003010011209", "000500301001120a", "00050030100112ff":
		return true
	default:
		return false
	}
}

func shouldKeepCodeFSTAndTMD(normTID string) bool {
	if strings.HasPrefix(normTID, "0005001b") {
		switch normTID {
		case "0005001b10067200", "0005001b10069200":
			return false
		default:
			return true
		}
	}
	switch normTID {
	case "0005001010041200", "0005001010041100", "0005001010041000",
		"0005001010043200", "0005001010043100", "0005001010043000",
		"000500101004e200", "000500101004e100", "000500101004e000",
		"0005001010066000",
		"0005003010011209", "0005003010011109", "0005003010011009",
		"000500301001a10a":
		return true
	}
	return false
}

func shouldTrimCodeTMD(normTID string) bool {
	switch normTID {
	case "0005001b10042400", "0005001b10056000", "0005001b10057000", "0005003010011209":
		return true
	default:
		return false
	}
}

func DecryptAndExtractTitle(workDir, targetDir string, titleID string) error {
	workDir = filepath.Clean(strings.TrimRight(workDir, "/\\"))
	targetDir = filepath.Clean(strings.TrimRight(targetDir, "/\\"))
	normTID := NormalizeTID(titleID)

	tmdPath := filepath.Join(workDir, "title.tmd")
	tikPath := filepath.Join(workDir, "title.tik")

	tmd, titleKey, err := ReadTMDAndTitleKey(tmdPath, tikPath, "")
	if err != nil {
		return err
	}

	titleBlock, err := aes.NewCipher(titleKey)
	if err != nil {
		return err
	}

	if IsRawTitle(normTID) {
		return ExtractRawTitle(targetDir, tmd, titleBlock)
	}

	rawTMD, err := os.ReadFile(tmdPath)
	if err != nil {
		return err
	}

	var c0 *Content
	for i := range tmd.Contents {
		if tmd.Contents[i].Index == 0 {
			c0 = &tmd.Contents[i]
			break
		}
	}
	if c0 == nil {
		return errors.New("content index 0 not found in TMD")
	}

	c0Path, err := resolveContentFile(workDir, c0.ID, ".app")
	if err != nil {
		return err
	}

	c0Enc, err := os.ReadFile(c0Path)
	if err != nil {
		return err
	}
	if len(c0Enc) == 0 {
		return errors.New("content index 0 is empty")
	}
	if rem := len(c0Enc) & 0xF; rem != 0 {
		c0Enc = append(c0Enc, make([]byte, 16-rem)...)
	}

	var zeroIV [16]byte
	fstDec := make([]byte, len(c0Enc))
	cipher.NewCBCDecrypter(titleBlock, zeroIV[:]).CryptBlocks(fstDec, c0Enc)
	if c0.Size > 0 && uint64(len(fstDec)) > c0.Size {
		fstDec = fstDec[:c0.Size]
	}

	codeDir := filepath.Join(targetDir, "code")
	if err := os.MkdirAll(codeDir, 0755); err != nil {
		return err
	}

	isFST := false
	var totalEntries uint32
	var entriesPtr, nameTableOff uint64
	var offsetFactor uint64 = 0x20

	if len(fstDec) >= 0x20 && binary.BigEndian.Uint32(fstDec[0x00:0x04]) == 0x46535400 {
		factor := binary.BigEndian.Uint32(fstDec[0x04:0x08])
		if factor != 0 {
			offsetFactor = uint64(factor)
		}

		infoCount := binary.BigEndian.Uint32(fstDec[0x08:0x0C])
		entriesPtr = uint64(0x20) + (uint64(infoCount) * 0x20)
		if entriesPtr+16 <= uint64(len(fstDec)) {
			totalEntries = binary.BigEndian.Uint32(fstDec[entriesPtr+8 : entriesPtr+12])
			nameTableOff = entriesPtr + (uint64(totalEntries) * 0x10)
			if nameTableOff <= uint64(len(fstDec)) && totalEntries > 0 && totalEntries < 100000 {
				isFST = true
			}
		}
	}

	if isFST {
		if shouldKeepCodeFSTAndTMD(normTID) {
			tmdToWrite := rawTMD
			if shouldTrimCodeTMD(normTID) {
				tmdToWrite = getTrimmedTMDBytes(rawTMD, tmd)
			}
			_ = os.WriteFile(filepath.Join(codeDir, "title.tmd"), tmdToWrite, 0644)
			_ = os.WriteFile(filepath.Join(codeDir, "title.fst"), fstDec, 0644)
		}

		contentMap := make(map[uint16]Content, len(tmd.Contents))
		for _, c := range tmd.Contents {
			contentMap[c.Index] = c
		}

		pathStack := []string{targetDir}
		endStack := []uint32{totalEntries}

		for i := uint32(1); i < totalEntries; i++ {
			for len(endStack) > 0 && i >= endStack[len(endStack)-1] {
				pathStack = pathStack[:len(pathStack)-1]
				endStack = endStack[:len(endStack)-1]
			}
			if len(pathStack) == 0 {
				return errors.New("corrupted FST directory stack")
			}

			off := entriesPtr + (uint64(i) * 0x10)
			if off+16 > uint64(len(fstDec)) {
				continue
			}

			eType := fstDec[off]
			eNameOff := (uint32(fstDec[off+1]) << 16) | (uint32(fstDec[off+2]) << 8) | uint32(fstDec[off+3])
			eOff := binary.BigEndian.Uint32(fstDec[off+4 : off+8])
			eLen := binary.BigEndian.Uint32(fstDec[off+8 : off+12])
			eFlags := binary.BigEndian.Uint16(fstDec[off+12 : off+14])
			eCID := binary.BigEndian.Uint16(fstDec[off+14 : off+16])

			nameStart := nameTableOff + uint64(eNameOff)
			if nameStart >= uint64(len(fstDec)) {
				continue
			}
			nameEnd := bytes.IndexByte(fstDec[nameStart:], 0)
			if nameEnd == -1 {
				continue
			}
			name := string(fstDec[nameStart : nameStart+uint64(nameEnd)])

			if !isSafeSingleName(name) {
				if (eType & 0x01) != 0 && eLen > i+1 && eLen <= totalEntries {
					i = eLen - 1
				}
				continue
			}

			currentPath := filepath.Join(pathStack[len(pathStack)-1], name)
			if !isPathInsideBase(targetDir, currentPath) {
				if (eType & 0x01) != 0 && eLen > i+1 && eLen <= totalEntries {
					i = eLen - 1
				}
				continue
			}

			if (eType & 0x01) != 0 {
				if eLen <= i || eLen > totalEntries {
					return fmt.Errorf("invalid FST directory boundary %d at entry %d", eLen, i)
				}
				if (eType & 0x80) == 0 {
					if err := os.MkdirAll(currentPath, 0755); err != nil {
						return err
					}
				}
				pathStack = append(pathStack, currentPath)
				endStack = append(endStack, eLen)
			} else if (eType & 0x80) == 0 {
				c, ok := contentMap[eCID]
				if !ok {
					continue
				}
				srcApp, err := resolveContentFile(workDir, c.ID, ".app")
				if err != nil {
					return err
				}

				var realOffset uint64
				if (eFlags & 0x04) != 0 {
					realOffset = uint64(eOff)
				} else {
					realOffset = uint64(eOff) * offsetFactor
				}

				if err := os.MkdirAll(filepath.Dir(currentPath), 0755); err != nil {
					return err
				}

				if strings.EqualFold(name, "bootLogoTex.tga") && shouldZeroBootLogo(normTID) {
					if err := os.WriteFile(currentPath, []byte{}, 0644); err != nil {
						return err
					}
					continue
				}

				if (c.Type & 0x02) != 0 {
					if err := extractFileHashed(srcApp, currentPath, titleBlock, c.Index, realOffset, uint64(eLen)); err != nil {
						return err
					}
				} else {
					if err := extractFileStandard(srcApp, currentPath, titleBlock, c.Index, realOffset, uint64(eLen)); err != nil {
						return err
					}
				}
			}
		}
	} else {
		for _, c := range tmd.Contents {
			srcApp, err := resolveContentFile(workDir, c.ID, ".app")
			if err != nil {
				return err
			}
			dstPath := filepath.Join(targetDir, fmt.Sprintf("%08X.app", c.ID))
			if srcApp != dstPath {
				_ = copyFile(srcApp, dstPath)
			}
		}
	}

	return nil
}

func GetMenuTIDLow(region string) string {
	switch region {
	case "USA":
		return "10040100"
	case "JPN":
		return "10040000"
	default:
		return "10040200"
	}
}

func GetMiiMakerTIDLow(region string) string {
	switch region {
	case "USA":
		return "1004a100"
	case "JPN":
		return "1004a000"
	default:
		return "1004a200"
	}
}

func GetDailyLogTIDLow(region string) string {
	switch region {
	case "USA":
		return "1004c100"
	case "JPN":
		return "1004c000"
	default:
		return "1004c200"
	}
}

func GetNotificationsTIDLow(region string) string {
	switch region {
	case "USA":
		return "1004d100"
	case "JPN":
		return "1004d000"
	default:
		return "1004d200"
	}
}

func GetSaveMetaTIDLows(region string) []string {
	return []string{
		GetMenuTIDLow(region),
		GetDailyLogTIDLow(region),
		GetNotificationsTIDLow(region),
		"10066000",
	}
}

func copySysMetaToSave(mlcRoot, tidLow string) (int, error) {
	srcDir := filepath.Join(mlcRoot, "sys", "title", "00050010", strings.ToUpper(tidLow), "meta")
	dstDir := filepath.Join(mlcRoot, "usr", "save", "00050010", strings.ToLower(tidLow), "meta")

	if _, err := os.Stat(srcDir); err != nil {
		return 0, nil
	}

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return 0, err
	}

	copied := 0
	for _, name := range []string{"iconTex.tga", "meta.xml"} {
		src := filepath.Join(srcDir, name)
		dst := filepath.Join(dstDir, name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return copied, err
		}
		if err := os.WriteFile(dst, data, 0644); err != nil {
			return copied, err
		}
		copied++
	}
	return copied, nil
}

func initMiiDatabaseFiles(dbDir string) error {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return err
	}

	writeIfMissing := func(path string, data []byte) error {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		return os.WriteFile(path, data, 0644)
	}

	fflODB := make([]byte, 276544)
	copy(fflODB[:4], []byte("FFOC"))
	if err := writeIfMissing(filepath.Join(dbDir, "FFL_ODB.dat"), fflODB); err != nil {
		return err
	}
	if err := writeIfMissing(filepath.Join(dbDir, "FFL_ODB_OLD.dat"), fflODB); err != nil {
		return err
	}

	fflHDB := make([]byte, 44032)
	if err := writeIfMissing(filepath.Join(dbDir, "FFL_HDB.dat"), fflHDB); err != nil {
		return err
	}

	fclDB := make([]byte, 524352)
	copy(fclDB[:4], []byte("FCDA"))
	for i := 4; i < len(fclDB); i++ {
		fclDB[i] = 0xFF
	}
	return writeIfMissing(filepath.Join(dbDir, "FCL_DB.dat"), fclDB)
}

func getPinnedTitleVersion(normTID string) int {
	switch strings.ToLower(normTID) {
	case "0005001010047200":
		return 178
	case "0005001010049200":
		return 103
	}
	return 0
}

func initSystemPDMData(mlcRoot string) error {
	pdmDir := filepath.Join(mlcRoot, "usr", "save", "system", "pdm", "80000001")
	if err := os.MkdirAll(pdmDir, 0755); err != nil {
		return err
	}
	diaryFile := filepath.Join(pdmDir, "PlayDiary.dat")
	if _, err := os.Stat(diaryFile); os.IsNotExist(err) {
		_ = os.WriteFile(diaryFile, make([]byte, 8), 0644)
	}
	statsFile := filepath.Join(pdmDir, "PlayStats.dat")
	if _, err := os.Stat(statsFile); os.IsNotExist(err) {
		statsData := make([]byte, 5124)
		statsData[0x3] = 0x02
		statsData[0x5] = 0x05
		statsData[0x7] = 0x10
		statsData[0x8] = 0x10
		statsData[0x9] = 0x04
		statsData[0xA] = 0x72
		statsData[0x11] = 0x01
		statsData[0x12] = 0x26
		statsData[0x13] = 0x3E
		statsData[0x14] = 0x26
		statsData[0x15] = 0x3E
		statsData[0x19] = 0x05
		statsData[0x1B] = 0x10
		statsData[0x1C] = 0x10
		statsData[0x1D] = 0x04
		statsData[0x1E] = 0x02
		statsData[0x25] = 0x03
		statsData[0x26] = 0x26
		statsData[0x27] = 0x3E
		statsData[0x28] = 0x26
		statsData[0x29] = 0x3E
		_ = os.WriteFile(statsFile, statsData, 0644)
	}
	return nil
}

func initSystemActData(mlcRoot string) error {
	for _, sysSub := range []string{"config", "import", "update"} {
		_ = os.MkdirAll(filepath.Join(mlcRoot, "sys", sysSub), 0755)
	}
	actFile := filepath.Join(mlcRoot, "usr", "save", "system", "act", "80000001", "account.dat")
	if data, err := os.ReadFile(actFile); err == nil {
		if !strings.Contains(string(data), "MiiData=") {
			_ = os.Remove(actFile)
		}
	}
	return nil
}

func SetupCemu2xSaveData(mlcRoot, region string) (int, error) {
	menuTID := GetMenuTIDLow(region)
	miiMakerTID := GetMiiMakerTIDLow(region)

	menuSaveDir := filepath.Join(mlcRoot, "usr", "save", "00050010", menuTID, "user")
	_ = os.MkdirAll(filepath.Join(menuSaveDir, "common"), 0755)
	_ = os.MkdirAll(filepath.Join(menuSaveDir, "80000001"), 0755)

	baristaDB := filepath.Join(menuSaveDir, "common", "BaristaIconDataBase.dat")
	if st, err := os.Stat(baristaDB); err == nil && st.Size() < 500000 {
		_ = os.Remove(baristaDB)
	}

	totalCopied := 0

	n, err := copySysMetaToSave(mlcRoot, menuTID)
	if err != nil {
		return totalCopied, fmt.Errorf("failed to copy Wii U Menu meta: %w", err)
	}
	totalCopied += n

	miiCommonDir := filepath.Join(mlcRoot, "usr", "save", "00050010", miiMakerTID, "user", "common")
	miiDBDir := filepath.Join(miiCommonDir, "db")
	if err := initMiiDatabaseFiles(miiDBDir); err != nil {
		return totalCopied, fmt.Errorf("failed to initialize Mii database: %w", err)
	}
	_ = initSystemActData(mlcRoot)

	_ = os.MkdirAll(filepath.Join(mlcRoot, "usr", "save", "00050010", miiMakerTID, "user", "80000001"), 0755)

	for _, tidLow := range []string{GetDailyLogTIDLow(region), GetNotificationsTIDLow(region), "10066000"} {
		n, err := copySysMetaToSave(mlcRoot, tidLow)
		if err != nil {
			return totalCopied, err
		}
		totalCopied += n
	}

	_ = initSystemPDMData(mlcRoot)

	return totalCopied, nil
}

func SetupCafeLibs(mlcRoot, cemuDir string) (int, error) {
	rplDir := filepath.Join(mlcRoot, "sys", "title", "00050010", "1000400A", "code")
	cafeLibsDir := filepath.Join(cemuDir, "cafeLibs")

	if err := os.MkdirAll(cafeLibsDir, 0755); err != nil {
		return 0, err
	}

	for _, extra := range []string{"nn_ec.rpl", "nn_fp.rpl", "nn_olv.rpl", "proc_ui.rpl"} {
		_ = os.Remove(filepath.Join(cafeLibsDir, extra))
	}

	rplFiles := []string{
		"drmapp.rpl", "nn_sl.rpl", "nsyskbd.rpl",
		"snd_user.rpl", "snduser2.rpl", "swkbd.rpl", "erreula.rpl",
	}

	count := 0
	for _, file := range rplFiles {
		src := filepath.Join(rplDir, file)
		dst := filepath.Join(cafeLibsDir, file)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		data, err := os.ReadFile(src)
		if err != nil {
			continue
		}
		if err := os.WriteFile(dst, data, 0644); err != nil {
			continue
		}
		count++
	}

	return count, nil
}

func CheckCafeLibsPresent(cemuDir string) bool {
	cafeLibsDir := filepath.Join(cemuDir, "cafeLibs")
	required := []string{"drmapp.rpl", "nn_sl.rpl", "erreula.rpl", "snd_user.rpl", "swkbd.rpl"}
	for _, rpl := range required {
		if _, err := os.Stat(filepath.Join(cafeLibsDir, rpl)); err != nil {
			return false
		}
	}
	return true
}

func ValidateCemu2xSetup(mlcRoot, cemuDir, region string) []string {
	var warnings []string
	menuTID := GetMenuTIDLow(region)
	miiMakerTID := GetMiiMakerTIDLow(region)

	menRpx := filepath.Join(mlcRoot, "sys", "title", "00050010", menuTID, "code", "men.rpx")
	if _, err := os.Stat(menRpx); err != nil {
		warnings = append(warnings, "Wii U Menu (men.rpx) not found")
	}

	rplDir := filepath.Join(mlcRoot, "sys", "title", "00050010", "1000400A", "code")
	for _, rpl := range []string{"nn_sl.rpl", "drmapp.rpl"} {
		if _, err := os.Stat(filepath.Join(rplDir, rpl)); err != nil {
			warnings = append(warnings, fmt.Sprintf("Required RPL %s not found in OSv10", rpl))
		}
	}

	cafeLibsDir := filepath.Join(cemuDir, "cafeLibs")
	for _, rpl := range []string{"drmapp.rpl", "nn_sl.rpl", "erreula.rpl", "snd_user.rpl", "swkbd.rpl"} {
		if _, err := os.Stat(filepath.Join(cafeLibsDir, rpl)); err != nil {
			warnings = append(warnings, fmt.Sprintf("cafeLibs missing: %s", rpl))
		}
	}

	saveDir := filepath.Join(mlcRoot, "usr", "save", "00050010", menuTID, "user")
	if _, err := os.Stat(saveDir); err != nil {
		warnings = append(warnings, "Wii U Menu save path not created")
	}

	miiDBDir := filepath.Join(mlcRoot, "usr", "save", "00050010", miiMakerTID, "user", "common", "db")
	for _, db := range []string{"FFL_ODB.dat", "FCL_DB.dat"} {
		if _, err := os.Stat(filepath.Join(miiDBDir, db)); err != nil {
			warnings = append(warnings, fmt.Sprintf("Mii database file missing: %s", db))
		}
	}

	for _, tidLow := range GetSaveMetaTIDLows(region) {
		dst := filepath.Join(mlcRoot, "usr", "save", "00050010", strings.ToLower(tidLow), "meta")
		for _, name := range []string{"iconTex.tga", "meta.xml"} {
			if _, err := os.Stat(filepath.Join(dst, name)); err != nil {
				warnings = append(warnings, fmt.Sprintf("Save meta missing: %s/%s", tidLow, name))
			}
		}
	}

	sharedTIDs := []string{
		"1004F000", "10050000", "10051000",
		"10057000", "10058000", "1005C000", "10063000",
	}
	for _, sTID := range sharedTIDs {
		dir := filepath.Join(mlcRoot, "sys", "title", "0005001B", sTID)
		if _, err := os.Stat(dir); err != nil {
			warnings = append(warnings, fmt.Sprintf("Shared data title missing: 0005001B%s", sTID))
		}
	}

	return warnings
}

func CleanRawNUSFiles(dir string, titleID string) {
	normTID := NormalizeTID(titleID)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	if normTID == "0005001010000400" || normTID == "0005001010066000" {
		_ = os.Remove(filepath.Join(dir, "title.cert"))
		return
	}

	if normTID == "0005001010000100" {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := strings.ToLower(e.Name())
			if strings.HasSuffix(name, ".app") ||
				strings.HasSuffix(name, ".h3") ||
				name == "title.tmd" ||
				name == "title.tik" ||
				name == "title.cert" {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
		return
	}

	hasExtracted := false
	for _, sub := range []string{"code", "content", "meta"} {
		if st, err := os.Stat(filepath.Join(dir, sub)); err == nil && st.IsDir() {
			hasExtracted = true
			break
		}
	}

	if hasExtracted {
		for _, e := range entries {
			if !e.IsDir() {
				name := strings.ToLower(e.Name())
				if strings.HasSuffix(name, ".app") ||
					strings.HasSuffix(name, ".h3") ||
					strings.HasSuffix(name, ".dec") ||
					name == "title.tmd" ||
					name == "title.tik" ||
					name == "title.cert" {
					_ = os.Remove(filepath.Join(dir, e.Name()))
				}
			}
		}
	}
}

func fetchTMDBytesWithFallback(ctx context.Context, client *http.Client, cleanTID string) ([]byte, error) {
	baseURL := fmt.Sprintf("http://ccs.cdn.c.shop.nintendowifi.net/ccs/download/%s", cleanTID)
	var urls []string
	if v := getPinnedTitleVersion(cleanTID); v > 0 {
		urls = append(urls, fmt.Sprintf("%s/tmd.%d", baseURL, v))
	}
	urls = append(urls, fmt.Sprintf("%s/tmd", baseURL))

	var lastErr error
	for _, u := range urls {
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u, nil)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		req.Header.Set("User-Agent", "WiiUDownloader")

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			cancel()
			lastErr = fmt.Errorf("HTTP %d for %s", resp.StatusCode, u)
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPMetadataBytes))
		resp.Body.Close()
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, lastErr
}

func GetTitleSize(titleID string, client *http.Client) (int64, error) {
	cleanTID := NormalizeTID(titleID)
	data, err := fetchTMDBytesWithFallback(context.Background(), client, cleanTID)
	if err != nil {
		return 0, err
	}

	tmd, err := ParseTMD(data)
	if err != nil {
		return 0, err
	}

	var total uint64
	for i := range tmd.Contents {
		total += tmd.Contents[i].Size
	}
	return int64(total), nil
}

func downloadFile(ctx context.Context, client *http.Client, downloadURL, dstPath string, onProgress func(int64)) error {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "WiiUDownloader")

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return err
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return fmt.Errorf("HTTP error %d for %s", resp.StatusCode, downloadURL)
		}

		file, err := os.Create(dstPath)
		if err != nil {
			resp.Body.Close()
			return err
		}

		var downloadedThisAttempt int64
		buf := make([]byte, 128*1024)
		var readErr error

		for {
			var n int
			n, readErr = resp.Body.Read(buf)
			if n > 0 {
				if _, writeErr := file.Write(buf[:n]); writeErr != nil {
					file.Close()
					resp.Body.Close()
					return writeErr
				}
				downloadedThisAttempt += int64(n)
				if onProgress != nil {
					onProgress(int64(n))
				}
			}
			if readErr != nil {
				break
			}
		}

		file.Close()
		resp.Body.Close()

		if errors.Is(readErr, io.EOF) {
			return nil
		}

		if downloadedThisAttempt > 0 && onProgress != nil {
			onProgress(-downloadedThisAttempt)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < maxRetries {
			time.Sleep(retryDelay)
			continue
		}
		return readErr
	}

	return fmt.Errorf("failed to download %s after %d retries", downloadURL, maxRetries)
}

func DownloadAndExtractTitle(
	ctx context.Context,
	titleID, outputPath string,
	client *http.Client,
	cleanInstall bool,
	onProgress func(int64),
) error {
	targetDir := strings.TrimRight(outputPath, "/\\")
	normTID := NormalizeTID(titleID)
	baseURL := fmt.Sprintf("http://ccs.cdn.c.shop.nintendowifi.net/ccs/download/%s", normTID)

	if cleanInstall {
		normTarget := filepath.Clean(targetDir)
		pathParts := strings.Split(normTarget, string(filepath.Separator))
		isSysTitle := false
		for _, part := range pathParts {
			if part == "sys" {
				isSysTitle = true
				break
			}
		}
		if isSysTitle {
			_ = os.RemoveAll(targetDir)
		}
	}

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return err
	}

	tmdPath := filepath.Join(targetDir, "title.tmd")
	var tmdErr error
	if v := getPinnedTitleVersion(normTID); v > 0 {
		pinnedURL := fmt.Sprintf("%s/tmd.%d", baseURL, v)
		tmdErr = downloadFile(ctx, client, pinnedURL, tmdPath, nil)
		if tmdErr != nil {
			tmdErr = downloadFile(ctx, client, fmt.Sprintf("%s/tmd", baseURL), tmdPath, nil)
		}
	} else {
		tmdErr = downloadFile(ctx, client, fmt.Sprintf("%s/tmd", baseURL), tmdPath, nil)
	}
	if tmdErr != nil {
		return tmdErr
	}

	tmdData, err := os.ReadFile(tmdPath)
	if err != nil {
		return err
	}

	tmd, err := ParseTMD(tmdData)
	if err != nil {
		return err
	}

	tikPath := filepath.Join(targetDir, "title.tik")
	if err := downloadFile(ctx, client, fmt.Sprintf("%s/cetk", baseURL), tikPath, nil); err != nil {
		return err
	}

	if err := generateCert(ctx, tmd, filepath.Join(targetDir, "title.cert"), client); err != nil {
		return err
	}

	useLowerCaseApp := normTID == "0005001010000400"

	type dlTask struct {
		url  string
		path string
	}
	var tasks []dlTask

	for _, c := range tmd.Contents {
		appFileName := fmt.Sprintf("%08X.app", c.ID)
		if useLowerCaseApp {
			appFileName = fmt.Sprintf("%08x.app", c.ID)
		} else if normTID == "0005001010066000" {
			_ = os.Remove(filepath.Join(targetDir, fmt.Sprintf("%08x.app", c.ID)))
		}

		tasks = append(tasks, dlTask{
			url:  fmt.Sprintf("%s/%08X", baseURL, c.ID),
			path: filepath.Join(targetDir, appFileName),
		})
		if (c.Type & 0x02) != 0 {
			h3FileName := fmt.Sprintf("%08X.h3", c.ID)
			if useLowerCaseApp {
				h3FileName = fmt.Sprintf("%08x.h3", c.ID)
			} else if normTID == "0005001010066000" {
				_ = os.Remove(filepath.Join(targetDir, fmt.Sprintf("%08x.h3", c.ID)))
			}
			tasks = append(tasks, dlTask{
				url:  fmt.Sprintf("%s/%08X.h3", baseURL, c.ID),
				path: filepath.Join(targetDir, h3FileName),
			})
		}
	}

	sem := make(chan struct{}, maxConcurrentDownloads)
	errChan := make(chan error, len(tasks))
	var wg sync.WaitGroup

	for _, t := range tasks {
		wg.Add(1)
		go func(task dlTask) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}

			if err := downloadFile(ctx, client, task.url, task.path, onProgress); err != nil {
				errChan <- err
			}
		}(t)
	}

	wg.Wait()
	close(errChan)

	for e := range errChan {
		if e != nil {
			return e
		}
	}

	if err := VerifyTitle(titleID, targetDir); err != nil {
		return fmt.Errorf("integrity check failed: %w", err)
	}

	if err := DecryptAndExtractTitle(targetDir, targetDir, titleID); err != nil {
		return fmt.Errorf("decryption/extraction failed: %w", err)
	}

	CleanRawNUSFiles(targetDir, titleID)

	if err := VerifyTitle(titleID, targetDir); err != nil {
		return fmt.Errorf("post-extraction verification failed: %w", err)
	}

	return nil
}
