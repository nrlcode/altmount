package health

import "github.com/javi11/nntppool/v4"

const (
	bodyValidationUUStructural = "uu_structural"
	bodyValidationYEncCRC      = "yenc_crc"
)

func bodyValidationMethod(result nntppool.BodyResult) string {
	if result.Err != nil || result.Body == nil {
		return ""
	}

	switch result.Body.Encoding {
	case nntppool.EncodingUU:
		return bodyValidationUUStructural
	case nntppool.EncodingYEnc:
		if result.Body.CRCProvided && result.Body.CRCValid {
			return bodyValidationYEncCRC
		}
	}

	return ""
}
