package main

import (
	"testing"
)

type testMergeDuplicates struct {
	name           string
	newEle         []string
	oldEle         []string
	expectedResult []string
}

func TestMergeDuplicatesAllowEmptyImport(t *testing.T) {
	e := &ExtSort{
		importEmpty: true,
	}
	for _, tc := range []testMergeDuplicates{
		{
			name:           "EmptyAttributesInNewElement",
			newEle:         []string{"1", "test@sendinblue.com", "", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "", "30"},
		},
		{
			name:           "EmptyAttributesInOldElement",
			newEle:         []string{"1", "test@sendinblue.com", "test", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test", "30"},
		},
		{
			name:           "EmptyAttributesInBothElements",
			newEle:         []string{"1", "test@sendinblue.com", "test", ""},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test", ""},
		},
		{
			name:           "NoEmptyAttributes",
			newEle:         []string{"1", "test@sendinblue.com", "test1", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test1", "30"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := e.getMergedValue(tc.newEle, tc.oldEle)
			for i, v := range actual {
				if tc.expectedResult[i] != v {
					t.Fatalf("incorrect merge, expected %v got %v", tc.expectedResult, actual)
				}
			}
		})
	}
}

func TestMergeDuplicatesNoEmptyImport(t *testing.T) {
	e := &ExtSort{
		importEmpty: false,
	}
	for _, tc := range []testMergeDuplicates{
		{
			name:           "EmptyAttributesInNewElement",
			newEle:         []string{"1", "test@sendinblue.com", "", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"2", "test@sendinblue.com", "test", "20"},
		},
		{
			name:           "EmptyAttributesInOldElement",
			newEle:         []string{"1", "test@sendinblue.com", "test1", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test1", "30"},
		},
		{
			name:           "EmptyAttributesInBothElements",
			newEle:         []string{"1", "test@sendinblue.com", "test", ""},
			oldEle:         []string{"2", "test@sendinblue.com", "", "20"},
			expectedResult: []string{"2", "test@sendinblue.com", "", "20"},
		},
		{
			name:           "NoEmptyAttributes",
			newEle:         []string{"1", "test@sendinblue.com", "test1", "30"},
			oldEle:         []string{"2", "test@sendinblue.com", "test", "20"},
			expectedResult: []string{"1", "test@sendinblue.com", "test1", "30"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := e.getMergedValue(tc.newEle, tc.oldEle)
			for i, v := range actual {
				if tc.expectedResult[i] != v {
					t.Fatalf("incorrect merge, expected %v got %v", tc.expectedResult, actual)
				}
			}
		})
	}
}
