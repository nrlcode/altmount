package health

import (
	"errors"
	"testing"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
)

func TestBodyValidationMethod(t *testing.T) {
	tests := []struct {
		name   string
		result nntppool.BodyResult
		want   string
	}{
		{
			name: "full UU article",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{
				Encoding: nntppool.EncodingUU, Bytes: []byte("decoded"), BytesDecoded: 7,
			}},
			want: "uu_structural",
		},
		{
			// Wrapperless middle parts are structurally validated upstream; a
			// streamed result intentionally has no locally assembled body bytes.
			name: "wrapperless middle UU part streamed",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{
				Encoding: nntppool.EncodingUU, BytesDecoded: 512,
			}},
			want: "uu_structural",
		},
		{
			name: "yEnc with valid supplied CRC",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{
				Encoding: nntppool.EncodingYEnc, CRCProvided: true, CRCValid: true,
			}},
			want: "yenc_crc",
		},
		{name: "nil body", result: nntppool.BodyResult{}},
		{name: "result error", result: nntppool.BodyResult{Err: errors.New("body failed")}},
		{
			name: "result error overrides positive body",
			result: nntppool.BodyResult{
				Body: &nntppool.ArticleBody{Encoding: nntppool.EncodingUU},
				Err:  errors.New("body failed"),
			},
		},
		{
			name: "unknown raw body ignores bytes and CRC flags",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{
				Encoding: nntppool.EncodingUnknown, Bytes: []byte("decoded"), CRCProvided: true, CRCValid: true,
			}},
		},
		{
			name: "result error overrides CRC-valid yEnc body",
			result: nntppool.BodyResult{
				Body: &nntppool.ArticleBody{Encoding: nntppool.EncodingYEnc, CRCProvided: true, CRCValid: true},
				Err:  errors.New("body failed"),
			},
		},
		{
			name:   "yEnc without CRC",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{Encoding: nntppool.EncodingYEnc}},
		},
		{
			name: "yEnc with invalid CRC",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{
				Encoding: nntppool.EncodingYEnc, CRCProvided: true,
			}},
		},
		{
			name: "yEnc valid flag without supplied CRC",
			result: nntppool.BodyResult{Body: &nntppool.ArticleBody{
				Encoding: nntppool.EncodingYEnc, CRCValid: true,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, bodyValidationMethod(tt.result))
		})
	}
}
