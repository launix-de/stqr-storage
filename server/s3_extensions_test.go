// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func startS3Multipart(t *testing.T, s *Server, key string, headers map[string]string) string {
	t.Helper()
	w := s3TestRequest(t, s, "POST", "/s3/fop-files/"+key+"?uploads", nil, headers)
	requireS3Status(t, w, 200)
	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if e := xml.Unmarshal(w.Body.Bytes(), &result); e != nil || result.UploadID == "" {
		t.Fatal(w.Body.String(), e)
	}
	return result.UploadID
}
func completeS3XML(parts ...client.S3CompletedPart) []byte {
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag>", p.Number, p.ETag)
		for algo, value := range p.Checksums {
			fmt.Fprintf(&b, "<Checksum%s>%s</Checksum%s>", algo, value, algo)
		}
		b.WriteString("</Part>")
	}
	b.WriteString("</CompleteMultipartUpload>")
	return []byte(b.String())
}
func TestS3MultipartCompleteAbortAndReclamation(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	first := bytes.Repeat([]byte("a"), 5<<20)
	last := []byte("last part")
	id := startS3Multipart(t, s, "large", nil)
	one := s3TestRequest(t, s, "PUT", "/s3/fop-files/large?uploadId="+id+"&partNumber=1", first, nil)
	requireS3Status(t, one, 200)
	two := s3TestRequest(t, s, "PUT", "/s3/fop-files/large?uploadId="+id+"&partNumber=2", last, nil)
	requireS3Status(t, two, 200)
	listed := s3TestRequest(t, s, "GET", "/s3/fop-files/large?uploadId="+id+"&max-parts=1", nil, nil)
	requireS3Status(t, listed, 200)
	if !strings.Contains(listed.Body.String(), "<IsTruncated>true</IsTruncated>") {
		t.Fatal(listed.Body.String())
	}
	requireS3Status(t, s3TestRequest(t, s, "GET", "/s3/fop-files/large", nil, nil), 404)
	request := completeS3XML(client.S3CompletedPart{Number: 1, ETag: one.Header().Get("ETag")}, client.S3CompletedPart{Number: 2, ETag: two.Header().Get("ETag")})
	completed := s3TestRequest(t, s, "POST", "/s3/fop-files/large?uploadId="+id, request, nil)
	requireS3Status(t, completed, 200)
	if !strings.Contains(completed.Body.String(), "-2") {
		t.Fatal(completed.Body.String())
	}
	got := s3TestRequest(t, s, "GET", "/s3/fop-files/large", nil, nil)
	requireS3Status(t, got, 200)
	if !bytes.Equal(got.Body.Bytes(), append(append([]byte(nil), first...), last...)) {
		t.Fatal("wrong assembled payload")
	}
	requireS3Status(t, s3TestRequest(t, s, "POST", "/s3/fop-files/large?uploadId="+id, request, nil), 200)
	// Complete drops all upload-part bytes, retaining only a small retry receipt.
	if physical := b.allVolumes()[0].Stats().DataPhysical; physical > uint64(len(first)+len(last))+(64<<10) {
		t.Fatal("multipart parts retained after complete", physical)
	}
	abort := startS3Multipart(t, s, "abort", nil)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/abort?uploadId="+abort+"&partNumber=1", first, nil), 200)
	requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/abort?uploadId="+abort, nil, nil), 204)
	requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/large", nil, nil), 204)
	if e := s.maintainS3(context.Background(), time.Now().Add(25*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if physical := b.allVolumes()[0].Stats().DataPhysical; physical != 0 {
		t.Fatal("abort/completion cleanup leaked data", physical)
	}
}
func TestS3MultipartValidationAndPartReplacement(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	id := startS3Multipart(t, s, "replace", nil)
	for i := 0; i < 24; i++ {
		requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/replace?uploadId="+id+"&partNumber=1", bytes.Repeat([]byte{byte(i)}, 5<<20), nil), 200)
	}
	if physical := b.allVolumes()[0].Stats().DataPhysical; physical > 6<<20 {
		t.Fatal("replaced parts retained", physical)
	}
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/replace?uploadId="+id+"&partNumber=2", []byte("short"), nil), 200)
	requireS3Status(t, s3TestRequest(t, s, "POST", "/s3/fop-files/replace?uploadId="+id, completeS3XML(client.S3CompletedPart{Number: 2, ETag: "bad"}, client.S3CompletedPart{Number: 1, ETag: "bad"}), nil), 400)
	requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/replace?uploadId="+id, nil, nil), 204)
	if b.allVolumes()[0].Stats().DataPhysical != 0 {
		t.Fatal("abort did not free parts")
	}
}

type multipartCrashStore struct {
	client.S3Store
	failDone atomic.Bool
}

func (s *multipartCrashStore) PutFrom(ctx context.Context, id client.ObjectID, src io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	prefix := make([]byte, 32)
	got, e := io.ReadFull(src, prefix)
	if e != nil && e != io.ErrUnexpectedEOF && e != io.EOF {
		return client.ObjectInfo{}, e
	}
	prefix = prefix[:got]
	if got >= 12 && string(prefix[:4]) == "STMU" && binary.LittleEndian.Uint32(prefix[8:12]) == client.S3UploadDone && s.failDone.CompareAndSwap(true, false) {
		return client.ObjectInfo{}, errors.New("simulated crash after target publication")
	}
	return s.S3Store.PutFrom(ctx, id, io.MultiReader(bytes.NewReader(prefix), src), n, opt)
}
func TestS3MultipartRecoveryCannotOverwriteLaterWrite(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	base := s3Store{b: b, p: userPrincipal(t, s)}
	crash := &multipartCrashStore{S3Store: base}
	api := client.NewS3(crash)
	u, e := api.CreateMultipart(ctx, "recover", user.ID, "", "", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	part, e := api.UploadPart(ctx, u.ID, 1, strings.NewReader("original completion"), 19, client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	crash.failDone.Store(true)
	parts := []client.S3CompletedPart{{Number: 1, ETag: part.ETag}}
	if _, e = api.CompleteMultipart(ctx, u.ID, parts, client.S3PutOptions{}); e == nil {
		t.Fatal("crash not injected")
	}
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/recover", []byte("newer write"), nil), 200)
	if e = s.maintainS3(ctx, time.Now()); e != nil {
		t.Fatal(e)
	}
	got := s3TestRequest(t, s, "GET", "/s3/fop-files/recover", nil, nil)
	if got.Body.String() != "newer write" {
		t.Fatal("recovery resurrected old content", got.Body.String())
	}
	completed, e := client.NewS3(base).Multipart(ctx, u.ID)
	if e != nil || completed.State != client.S3UploadDone || len(completed.Parts) != 0 {
		t.Fatal(completed.State, e)
	}
}
func TestS3CopyBulkDeleteAndBucketDiscovery(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/source", []byte("copy this"), map[string]string{"X-Amz-Meta-Test": "source"}), 200)
	copied := s3TestRequest(t, s, "PUT", "/s3/fop-files/copied", nil, map[string]string{"X-Amz-Copy-Source": "/fop-files/source"})
	requireS3Status(t, copied, 200)
	got := s3TestRequest(t, s, "GET", "/s3/fop-files/copied", nil, nil)
	if got.Body.String() != "copy this" || got.Header()["x-amz-meta-test"][0] != "source" {
		t.Fatal("copy failed")
	}
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/copied", nil, map[string]string{"X-Amz-Copy-Source": "/fop-files/source", "X-Amz-Metadata-Directive": "REPLACE", "X-Amz-Meta-Test": "replacement"}), 200)
	got = s3TestRequest(t, s, "GET", "/s3/fop-files/copied", nil, nil)
	if got.Header()["x-amz-meta-test"][0] != "replacement" {
		t.Fatal("metadata replace failed")
	}
	for _, path := range []string{"/s3", "/s3/fop-files?location", "/s3/fop-files?versioning"} {
		requireS3Status(t, s3TestRequest(t, s, "GET", path, nil, nil), 200)
	}
	body := []byte("<Delete><Object><Key>source</Key></Object><Object><Key>copied</Key></Object><Object><Key>missing</Key></Object></Delete>")
	sum := md5.Sum(body)
	deleted := s3TestRequest(t, s, "POST", "/s3/fop-files?delete", body, map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(sum[:])})
	requireS3Status(t, deleted, 200)
	if strings.Count(deleted.Body.String(), "<Deleted>") != 3 {
		t.Fatal(deleted.Body.String())
	}
	if b.allVolumes()[0].Stats().DataPhysical != 0 {
		t.Fatal("bulk delete leaked data")
	}
}
func TestS3CRC64NVMEVector(t *testing.T) {
	h, e := client.S3Checksum("CRC64NVME")
	if e != nil {
		t.Fatal(e)
	}
	h.Write([]byte("123456789"))
	if hex.EncodeToString(h.Sum(nil)) != "ae8b14860a799888" {
		t.Fatal(hex.EncodeToString(h.Sum(nil)))
	}
}
func TestS3RealCLI(t *testing.T) {
	aws := os.Getenv("STQR_TEST_AWS_CLI")
	rclone := os.Getenv("STQR_TEST_RCLONE")
	if aws == "" && rclone == "" {
		t.Skip("set STQR_TEST_AWS_CLI / STQR_TEST_RCLONE for real clients")
	}
	s, _, _ := s3TestSetup(t)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	dir := t.TempDir()
	file := filepath.Join(dir, "body")
	data := bytes.Repeat([]byte("real client data\n"), 1<<20)
	if e := os.WriteFile(file, data, 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if aws != "" {
		run := func(args ...string) {
			t.Helper()
			cmd := exec.CommandContext(ctx, aws, append([]string{"--endpoint-url", host.URL + "/s3", "--region", "eu-west-1", "--no-cli-pager"}, args...)...)
			cmd.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID="+testS3Access, "AWS_SECRET_ACCESS_KEY="+testS3Secret, "AWS_EC2_METADATA_DISABLED=true", "AWS_PAGER=", "AWS_CONFIG_FILE="+filepath.Join(dir, "aws-config"))
			if out, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("aws %v: %v\n%s", args, e, out)
			}
		}
		run("s3", "ls")
		run("s3", "cp", file, "s3://fop-files/aws", "--only-show-errors")
		run("s3", "cp", "s3://fop-files/aws", "s3://fop-files/aws-copy", "--only-show-errors")
		download := filepath.Join(dir, "aws-down")
		run("s3", "cp", "s3://fop-files/aws-copy", download, "--only-show-errors")
		got, _ := os.ReadFile(download)
		if !bytes.Equal(got, data) {
			t.Fatal("aws payload mismatch")
		}
		run("s3", "rm", "s3://fop-files/", "--recursive", "--only-show-errors")
	}
	if rclone != "" {
		config := filepath.Join(dir, "rclone.conf")
		text := "[stqr]\ntype = s3\nprovider = Other\naccess_key_id = " + testS3Access + "\nsecret_access_key = " + testS3Secret + "\nendpoint = " + host.URL + "/s3\nregion = eu-west-1\nforce_path_style = true\nno_check_bucket = true\n"
		if e := os.WriteFile(config, []byte(text), 0600); e != nil {
			t.Fatal(e)
		}
		run := func(args ...string) {
			t.Helper()
			cmd := exec.CommandContext(ctx, rclone, append([]string{"--config", config, "--s3-upload-cutoff", "5M", "--s3-chunk-size", "5M"}, args...)...)
			if out, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("rclone %v: %v\n%s", args, e, out)
			}
		}
		run("copyto", file, "stqr:fop-files/rclone")
		run("copyto", "stqr:fop-files/rclone", "stqr:fop-files/rclone-copy")
		download := filepath.Join(dir, "rclone-down")
		run("copyto", "stqr:fop-files/rclone-copy", download)
		got, _ := os.ReadFile(download)
		if !bytes.Equal(got, data) {
			t.Fatal("rclone payload mismatch")
		}
		run("delete", "stqr:fop-files")
	}
}

// AWS's published streaming-trailer vector, independent of our request signer.
func TestS3OfficialStreamingTrailerVector(t *testing.T) {
	key := s3HMAC([]byte("AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"), "20130524")
	key = s3HMAC(key, "us-east-1")
	key = s3HMAC(key, "s3")
	key = s3HMAC(key, "aws4_request")
	body := "10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n" + strings.Repeat("a", 65536) + "\r\n400;chunk-signature=1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7\r\n" + strings.Repeat("a", 1024) + "\r\n0;chunk-signature=2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992\r\nx-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n\r\n"
	r := httptest.NewRequest(http.MethodPut, "http://localhost", strings.NewReader(body))
	r.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	r.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	r.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	r = r.WithContext(context.WithValue(r.Context(), s3SigningKey{}, s3Signing{key: key, date: "20130524T000000Z", scope: "20130524/us-east-1/s3/aws4_request", seed: "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"}))
	var out bytes.Buffer
	n, e := s3DecodeChunks(r, &out)
	if e != nil || n != 66560 || out.String() != strings.Repeat("a", 66560) {
		t.Fatal(n, e)
	}
}
func streamingS3Request(t *testing.T, mode string, data []byte, tamper string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "http://localhost/s3/fop-files/stream", nil)
	r.RemoteAddr = "127.0.0.1:1"
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(data)))
	hasTrailer := strings.HasSuffix(mode, "-TRAILER")
	if hasTrailer {
		r.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc64nvme")
	}
	signS3Payload(r, mode, testS3Secret)
	var body strings.Builder
	signed := mode != "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	date := r.Header.Get("X-Amz-Date")
	scope := date[:8] + "/eu-west-1/s3/aws4_request"
	key := s3HMAC([]byte("AWS4"+testS3Secret), date[:8])
	key = s3HMAC(key, "eu-west-1")
	key = s3HMAC(key, "s3")
	key = s3HMAC(key, "aws4_request")
	_, previous, _ := strings.Cut(r.Header.Get("Authorization"), "Signature=")
	for _, chunk := range [][]byte{data, nil} {
		if signed {
			sign := hex.EncodeToString(s3HMAC(key, "AWS4-HMAC-SHA256-PAYLOAD\n"+date+"\n"+scope+"\n"+previous+"\n"+s3SHA("")+"\n"+s3SHA(string(chunk))))
			fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(chunk), sign)
			previous = sign
		} else {
			fmt.Fprintf(&body, "%x\r\n", len(chunk))
		}
		if len(chunk) > 0 {
			body.Write(chunk)
			body.WriteString("\r\n")
		}
	}
	if hasTrailer {
		h, _ := client.S3Checksum("CRC64NVME")
		h.Write(data)
		checksum := base64.StdEncoding.EncodeToString(h.Sum(nil))
		line := "x-amz-checksum-crc64nvme:" + checksum
		body.WriteString(line + "\r\n")
		if signed {
			signature := hex.EncodeToString(s3HMAC(key, "AWS4-HMAC-SHA256-TRAILER\n"+date+"\n"+scope+"\n"+previous+"\n"+s3SHA(line+"\n")))
			body.WriteString("x-amz-trailer-signature:" + signature + "\r\n")
		}
	}
	body.WriteString("\r\n")
	wire := body.String()
	if tamper == "data" {
		wire = strings.Replace(wire, string(data), strings.Repeat("z", len(data)), 1)
	}
	r.Body = io.NopCloser(strings.NewReader(wire))
	r.ContentLength = int64(len(wire))
	return r
}
func TestS3StreamingModesAndTampering(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	for _, mode := range []string{"STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"} {
		r := streamingS3Request(t, mode, []byte("streamed payload"), "")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		requireS3Status(t, w, 200)
		got := s3TestRequest(t, s, "GET", "/s3/fop-files/stream", nil, nil)
		if got.Body.String() != "streamed payload" {
			t.Fatal(got.Body.String())
		}
	}
	before := b.allVolumes()[0].Stats().DataPhysical
	for _, mode := range []string{"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"} {
		r := streamingS3Request(t, mode, []byte("streamed payload"), "data")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 400 && w.Code != 403 {
			t.Fatal("tampered stream accepted", w.Code, w.Body.String())
		}
	}
	if b.allVolumes()[0].Stats().DataPhysical != before {
		t.Fatal("tampered stream changed storage")
	}
}

type multipartBeforePublishStore struct {
	client.S3Store
	target client.ObjectID
}

func (s multipartBeforePublishStore) PutFrom(ctx context.Context, id client.ObjectID, r io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	if id == s.target {
		return client.ObjectInfo{}, errors.New("simulated loss before publication")
	}
	return s.S3Store.PutFrom(ctx, id, r, n, opt)
}
func TestS3MultipartRestartRecoversPendingCompletion(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	api := client.NewS3(multipartBeforePublishStore{S3Store: s3Store{b: b, p: userPrincipal(t, s)}, target: client.S3MetadataID("restart")})
	u, e := api.CreateMultipart(ctx, "restart", user.ID, "CRC64NVME", "FULL_OBJECT", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	part, e := api.UploadPart(ctx, u.ID, 1, strings.NewReader("restart payload"), 15, client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = api.CompleteMultipart(ctx, u.ID, []client.S3CompletedPart{{Number: 1, ETag: part.ETag}}, client.S3PutOptions{}); e == nil {
		t.Fatal("failure not injected")
	}
	config := s.configPath
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(Options{ConfigPath: config})
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	bucket, e := reopened.Bucket(b.ID())
	if e != nil {
		t.Fatal(e)
	}
	recovered := client.NewS3(s3Store{b: bucket, p: userPrincipal(t, reopened)})
	deadline := time.Now().Add(5 * time.Second)
	for {
		record, e := recovered.Multipart(ctx, u.ID)
		if e == nil && record.State == client.S3UploadDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("startup recovery did not finish", record.State, e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := s3TestRequest(t, reopened, "GET", "/s3/fop-files/restart", nil, nil)
	requireS3Status(t, got, 200)
	if got.Body.String() != "restart payload" {
		t.Fatal(got.Body.String())
	}
}
func TestS3MultipartRecoveryRespectsRevokedGrants(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	base := s3Store{b: b, p: userPrincipal(t, s)}
	api := client.NewS3(multipartBeforePublishStore{S3Store: base, target: client.S3MetadataID("revoked")})
	u, e := api.CreateMultipart(ctx, "revoked", user.ID, "", "", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	part, e := api.UploadPart(ctx, u.ID, 1, strings.NewReader("not authorized anymore"), 22, client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = api.CompleteMultipart(ctx, u.ID, []client.S3CompletedPart{{Number: 1, ETag: part.ETag}}, client.S3PutOptions{}); e == nil {
		t.Fatal("failure not injected")
	}
	enc := client.Encoder{}
	enc.ID(client.ID(b.ID()))
	uid, _ := parseID(user.ID)
	enc.ID(client.ID(uid))
	if _, status, _ := adminBucketMessage(t, s, client.BucketRevoke, enc.B); status != 200 {
		t.Fatal(status)
	}
	if e = s.maintainS3(ctx, time.Now()); e != nil {
		t.Fatal(e)
	}
	admin := client.NewS3(base)
	if _, e = admin.Head(ctx, "revoked"); !errors.Is(e, client.ErrNotFound) {
		t.Fatal("revoked completion published", e)
	}
	record, e := admin.Multipart(ctx, u.ID)
	if e != nil || record.State != client.S3UploadFailed || len(record.Parts) != 0 {
		t.Fatal("revoked parts retained", record.State, e)
	}
}
func TestS3InvalidCompositeChecksumCanBeRetriedOrAborted(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	api := client.NewS3(s3Store{b: b, p: userPrincipal(t, s)})
	u, e := api.CreateMultipart(ctx, "bad-complete", user.ID, "CRC32", "COMPOSITE", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	part, e := api.UploadPart(ctx, u.ID, 1, strings.NewReader("checksum"), 8, client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	_, e = api.CompleteMultipart(ctx, u.ID, []client.S3CompletedPart{{Number: 1, ETag: part.ETag}}, client.S3PutOptions{Checksums: map[string]string{"CRC32": "AAAAAA==-1"}})
	if !errors.Is(e, client.ErrS3Checksum) {
		t.Fatal(e)
	}
	record, e := api.Multipart(ctx, u.ID)
	if e != nil || record.State != client.S3UploadOpen {
		t.Fatal("bad checksum locked the upload", record.State, e)
	}
	if e = api.AbortMultipart(ctx, u.ID); e != nil {
		t.Fatal(e)
	}
	if b.allVolumes()[0].Stats().DataPhysical != 0 {
		t.Fatal("bad completion/abort leaked data")
	}
}

type multipartFullStore struct {
	client.S3Store
	target client.ObjectID
}

func (s multipartFullStore) PutFrom(ctx context.Context, id client.ObjectID, r io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	if id == s.target {
		return client.ObjectInfo{}, client.ErrFull
	}
	return s.S3Store.PutFrom(ctx, id, r, n, opt)
}
func TestS3FailedCompletionCanBeAbortedWhenStorageIsFull(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	api := client.NewS3(multipartFullStore{S3Store: s3Store{b: b, p: userPrincipal(t, s)}, target: client.S3MetadataID("full")})
	u, e := api.CreateMultipart(ctx, "full", user.ID, "", "", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	part, e := api.UploadPart(ctx, u.ID, 1, strings.NewReader("space constrained"), 17, client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = api.CompleteMultipart(ctx, u.ID, []client.S3CompletedPart{{Number: 1, ETag: part.ETag}}, client.S3PutOptions{}); !errors.Is(e, client.ErrFull) {
		t.Fatal(e)
	}
	record, e := api.Multipart(ctx, u.ID)
	if e != nil || record.State != client.S3UploadOpen {
		t.Fatal("full storage wedged an unabortable upload", record.State, e)
	}
	if e = api.AbortMultipart(ctx, u.ID); e != nil {
		t.Fatal(e)
	}
	if b.allVolumes()[0].Stats().DataPhysical != 0 {
		t.Fatal("parts leaked after full-storage completion/abort")
	}
}
func TestS3MaintenanceIgnoresUnrelatedNativeData(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	base := s3Store{b: b, p: userPrincipal(t, s)}
	var id client.ObjectID
	id[0] = 99
	raw := append([]byte("STMU"), bytes.Repeat([]byte("arbitrary database data"), 30)...)
	if _, e := base.PutFrom(ctx, id, bytes.NewReader(raw), uint64(len(raw)), client.PutOptions{RequestID: client.NewRequestID()}); e != nil {
		t.Fatal(e)
	}
	api := client.NewS3(base)
	u, e := api.CreateMultipart(ctx, "expired", user.ID, "", "", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = api.UploadPart(ctx, u.ID, 1, strings.NewReader("stale"), 5, client.S3PutOptions{}); e != nil {
		t.Fatal(e)
	}
	if e = s.maintainS3(ctx, time.Now().Add(25*time.Hour)); e != nil {
		t.Fatal(e)
	}
	got, _, e := base.Get(ctx, id)
	if e != nil || !bytes.Equal(got, raw) {
		t.Fatal("maintenance touched unrelated data", e)
	}
	if _, e = api.Multipart(ctx, u.ID); !errors.Is(e, client.ErrS3Upload) {
		t.Fatal("expired upload retained", e)
	}
}
func TestS3MultipartRejectsUnsupportedCompletionPrecondition(t *testing.T) {
	s, _, _ := s3TestSetup(t)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/conditional", []byte("existing"), nil), 200)
	id := startS3Multipart(t, s, "conditional", nil)
	part := s3TestRequest(t, s, "PUT", "/s3/fop-files/conditional?uploadId="+id+"&partNumber=1", []byte("new"), nil)
	requireS3Status(t, part, 200)
	complete := completeS3XML(client.S3CompletedPart{Number: 1, ETag: part.Header().Get("ETag")})
	requireS3Status(t, s3TestRequest(t, s, "POST", "/s3/fop-files/conditional?uploadId="+id, complete, map[string]string{"If-None-Match": "\"not-a-supported-condition\""}), 400)
	got := s3TestRequest(t, s, "GET", "/s3/fop-files/conditional", nil, nil)
	if got.Body.String() != "existing" {
		t.Fatal("unsupported condition silently overwrote target")
	}
	requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/conditional?uploadId="+id, nil, nil), 204)
}
