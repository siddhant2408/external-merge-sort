package extsort

import (
	"testing"
)

type testMergeDuplicates struct {
	name           string
	newEle         []string
	oldEle         []string
	expectedResult []string
	importEmpty    bool
}

func TestMergeDuplicates(t *testing.T) {
	e := &ExtSort{}
	for _, tc := range []testMergeDuplicates{
		{
			name:           "EmptyAttributesInNewElementAllowEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "", "30"},
			importEmpty:    true,
		},
		{
			name:           "EmptyAttributesInOldElementAllowEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "test", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test", "30"},
			importEmpty:    true,
		},
		{
			name:           "EmptyAttributesInBothElementsAllowEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "test", ""},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test", ""},
			importEmpty:    true,
		},
		{
			name:           "NoEmptyAttributesAllowEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "test1", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test1", "30"},
			importEmpty:    true,
		},
		{
			name:           "EmptyAttributesInNewElementNoEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test", "30"},
		},
		{
			name:           "EmptyAttributesInOldElementNoEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "test1", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test1", "30"},
		},
		{
			name:           "EmptyAttributesInBothElementsNoEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "test", ""},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test", "20"},
		},
		{
			name:           "NoEmptyAttributesNoEmptyImport",
			newEle:         []string{"1", "test@sendinblue.com", "test1", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test1", "30"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.ImportEmpty = tc.importEmpty
			actual := e.getMergedValue(tc.newEle, tc.oldEle)
			for i, v := range actual {
				if tc.expectedResult[i] != v {
					t.Fatalf("incorrect merge, expected %v got %v", tc.expectedResult, actual)
				}
			}
		})
	}
}
