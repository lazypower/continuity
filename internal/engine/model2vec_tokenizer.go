package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

// wordpieceTokenizer is a from-scratch, pure-Go reimplementation of the
// Hugging Face `tokenizers` crate's BERT pipeline (BertNormalizer +
// BertPreTokenizer + WordPiece model), built to match potion-retrieval-32M's
// tokenizer.json byte-for-byte. This is the "hard part" of model2vec support:
// the embedding matrix is a plain lookup table, but getting the SAME token ids
// as Python's tokenizer for arbitrary text requires reimplementing this
// pipeline exactly. See internal/engine/testdata/model2vec/parity.json for
// the fixture this is checked against (real model2vec output).
//
// add_special_tokens=false throughout: model2vec's StaticModel.tokenize/encode
// never add [CLS]/[SEP], so neither does this.
type wordpieceTokenizer struct {
	vocab                   map[string]int32 // token string -> id
	unkID                   int32
	continuingSubwordPrefix string
	maxInputCharsPerWord    int
}

// tokenizerJSON mirrors the subset of tokenizer.json this package reads.
type tokenizerJSON struct {
	Normalizer struct {
		Type               string `json:"type"`
		CleanText          bool   `json:"clean_text"`
		HandleChineseChars bool   `json:"handle_chinese_chars"`
		StripAccents       *bool  `json:"strip_accents"`
		Lowercase          bool   `json:"lowercase"`
	} `json:"normalizer"`
	PreTokenizer struct {
		Type string `json:"type"`
	} `json:"pre_tokenizer"`
	Model struct {
		Type                    string           `json:"type"`
		UnkToken                string           `json:"unk_token"`
		ContinuingSubwordPrefix string           `json:"continuing_subword_prefix"`
		MaxInputCharsPerWord    int              `json:"max_input_chars_per_word"`
		Vocab                   map[string]int32 `json:"vocab"`
	} `json:"model"`
}

// loadWordpieceTokenizer reads tokenizer.json and asserts the pipeline is
// exactly the one this implementation was built for (BertNormalizer with
// clean_text/handle_chinese_chars/lowercase all true and strip_accents null,
// BertPreTokenizer, WordPiece model). Any deviation fails closed rather than
// silently producing wrong token ids — a mismatched tokenizer would corrupt
// every embedding derived from it.
func loadWordpieceTokenizer(path string) (*wordpieceTokenizer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read tokenizer.json: %w", err)
	}
	var tj tokenizerJSON
	if err := json.Unmarshal(raw, &tj); err != nil {
		return nil, fmt.Errorf("parse tokenizer.json: %w", err)
	}

	if tj.Normalizer.Type != "BertNormalizer" {
		return nil, fmt.Errorf("unsupported normalizer %q; only BertNormalizer is implemented", tj.Normalizer.Type)
	}
	if !tj.Normalizer.CleanText || !tj.Normalizer.HandleChineseChars || !tj.Normalizer.Lowercase {
		return nil, fmt.Errorf("unsupported BertNormalizer settings (clean_text=%v handle_chinese_chars=%v lowercase=%v); "+
			"this implementation only supports clean_text=true handle_chinese_chars=true lowercase=true",
			tj.Normalizer.CleanText, tj.Normalizer.HandleChineseChars, tj.Normalizer.Lowercase)
	}
	if tj.Normalizer.StripAccents != nil {
		return nil, fmt.Errorf("unsupported strip_accents=%v; only strip_accents=null (implied strip, since lowercase=true) is implemented", *tj.Normalizer.StripAccents)
	}
	if tj.PreTokenizer.Type != "BertPreTokenizer" {
		return nil, fmt.Errorf("unsupported pre_tokenizer %q; only BertPreTokenizer is implemented", tj.PreTokenizer.Type)
	}
	if tj.Model.Type != "WordPiece" {
		return nil, fmt.Errorf("unsupported model type %q; only WordPiece is implemented", tj.Model.Type)
	}
	if len(tj.Model.Vocab) == 0 {
		return nil, fmt.Errorf("tokenizer.json model.vocab is empty")
	}

	unkID, ok := tj.Model.Vocab[tj.Model.UnkToken]
	if !ok {
		return nil, fmt.Errorf("unk_token %q not found in vocab", tj.Model.UnkToken)
	}

	maxChars := tj.Model.MaxInputCharsPerWord
	if maxChars <= 0 {
		maxChars = 100 // documented default
	}

	return &wordpieceTokenizer{
		vocab:                   tj.Model.Vocab,
		unkID:                   unkID,
		continuingSubwordPrefix: tj.Model.ContinuingSubwordPrefix,
		maxInputCharsPerWord:    maxChars,
	}, nil
}

// Encode runs the full BERT pipeline (normalize -> pre-tokenize -> WordPiece)
// over text and returns token ids with add_special_tokens=false semantics
// (no [CLS]/[SEP]). Callers that need model2vec's post-UNK-filter ids (i.e.
// what StaticModel.tokenize()/encode() consume) should filter unkID out
// themselves — kept as a separate step here so tests can observe raw ids too.
func (t *wordpieceTokenizer) Encode(text string) []int32 {
	normalized := bertNormalize(text)
	words := bertPreTokenize(normalized)

	var ids []int32
	for _, w := range words {
		ids = append(ids, t.wordpieceTokenize(w)...)
	}
	return ids
}

// bertNormalize implements BertNormalizer{clean_text, handle_chinese_chars,
// strip_accents (implied by lowercase), lowercase}, in the same order the HF
// `tokenizers` crate applies them: clean_text, then handle_chinese_chars,
// then strip_accents, then lowercase.
func bertNormalize(text string) string {
	text = cleanText(text)
	text = padChineseChars(text)
	text = stripAccentsNFD(text)
	text = strings.ToLower(text)
	return text
}

// cleanText drops null/replacement/control-or-format codepoints and maps all
// whitespace (including \t\n\r) to a single ASCII space, matching HF's
// BertNormalizer clean_text step.
func cleanText(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		switch {
		case r == 0 || r == 0xFFFD:
			continue
		case isControlOrFormat(r):
			continue
		case isWhitespaceRune(r):
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// isControlOrFormat reports whether r is a Unicode control (Cc) or format
// (Cf) category codepoint, EXCLUDING whitespace (which is handled/mapped
// separately by isWhitespaceRune — \t\n\r are Cc but must become a space, not
// be dropped).
func isControlOrFormat(r rune) bool {
	if isWhitespaceRune(r) {
		return false
	}
	return unicode.IsControl(r) || unicode.In(r, unicode.Cf)
}

// isWhitespaceRune reports whether r is whitespace under HF's BertNormalizer
// clean_text semantics (Unicode whitespace plus the ASCII control whitespace
// \t \n \r).
func isWhitespaceRune(r rune) bool {
	switch r {
	case '\t', '\n', '\r':
		return true
	}
	return unicode.IsSpace(r)
}

// cjkRanges are the codepoint ranges BertNormalizer's handle_chinese_chars
// surrounds with spaces (mirrors the HF `tokenizers` crate's is_chinese_char).
var cjkRanges = [][2]rune{
	{0x4E00, 0x9FFF},
	{0x3400, 0x4DBF},
	{0x20000, 0x2A6DF},
	{0x2A700, 0x2B73F},
	{0x2B740, 0x2B81F},
	{0x2B820, 0x2CEAF},
	{0xF900, 0xFAFF},
	{0x2F800, 0x2FA1F},
}

func isCJK(r rune) bool {
	for _, rg := range cjkRanges {
		if r >= rg[0] && r <= rg[1] {
			return true
		}
	}
	return false
}

// padChineseChars surrounds every CJK codepoint with ASCII spaces.
func padChineseChars(text string) string {
	var b strings.Builder
	b.Grow(len(text) + 8)
	for _, r := range text {
		if isCJK(r) {
			b.WriteByte(' ')
			b.WriteRune(r)
			b.WriteByte(' ')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// stripAccentsNFD NFD-decomposes text and drops combining marks (Unicode
// category Mn), implementing HF's strip_accents=true behavior — which is what
// strip_accents=null means whenever lowercase=true (HF semantics: null defers
// to "strip iff lowercasing").
func stripAccentsNFD(text string) string {
	// unicode/norm is not available without adding golang.org/x/text (a new
	// dependency, which is out of scope), so NFD decomposition is done by hand
	// via decompositionTable (model2vec_decomposition.go), a generated table
	// covering Latin-1 Supplement, Latin Extended-A/B, and Latin Extended
	// Additional — the entire practical surface for this tokenizer's inputs
	// (café, résumé, Zürich, Москва is unaffected since Cyrillic has no
	// decomposition here). Codepoints with no table entry pass through
	// unchanged (nothing to strip). Standalone combining marks already present
	// in the input (category Mn) are dropped directly too.
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		if unicode.Is(unicode.Mn, r) {
			continue // bare combining mark in the input — drop it
		}
		decomposed, ok := decompositionTable[r]
		if !ok {
			b.WriteRune(r)
			continue
		}
		for _, d := range decomposed {
			if unicode.Is(unicode.Mn, d) {
				continue // drop combining mark produced by decomposition
			}
			b.WriteRune(d)
		}
	}
	return b.String()
}

// bertPreTokenize implements BertPreTokenizer: split on whitespace, then
// split again so every punctuation rune becomes its own single-rune token.
// Punctuation = ASCII punctuation/symbol set the HF crate's is_punctuation
// checks (treated as punctuation regardless of Unicode category) plus any
// rune in a Unicode P* (Punctuation) category.
func bertPreTokenize(text string) []string {
	var words []string
	for _, ws := range strings.Fields(text) {
		words = append(words, splitOnPunctuation(ws)...)
	}
	return words
}

// asciiPunctuation is the literal HF `tokenizers` crate ASCII punctuation set
// treated as punctuation regardless of Unicode category (some of these, like
// '$' and '+', are Unicode Sc/Sm "symbol", not "punctuation").
const asciiPunctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

func isPunctuationRune(r rune) bool {
	if r < utf8.RuneSelf && strings.ContainsRune(asciiPunctuation, r) {
		return true
	}
	return unicode.IsPunct(r)
}

// splitOnPunctuation splits a single whitespace-delimited word into tokens,
// making every punctuation rune its own token and keeping runs of non-punct
// runes together.
func splitOnPunctuation(word string) []string {
	var tokens []string
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	for _, r := range word {
		if isPunctuationRune(r) {
			flush()
			tokens = append(tokens, string(r))
		} else {
			current.WriteRune(r)
		}
	}
	flush()
	return tokens
}

// wordpieceTokenize applies greedy longest-match-first WordPiece tokenization
// to a single pre-tokenized word, per the spec:
//
//	if rune-len(word) > maxInputCharsPerWord: [UNK]
//	else: start=0; repeatedly find the LONGEST vocab match of
//	  word[start:end] (prefixed with "##" when start>0); if none matches at
//	  some start, the WHOLE word becomes [UNK] (not just the remainder).
func (t *wordpieceTokenizer) wordpieceTokenize(word string) []int32 {
	runes := []rune(word)
	if len(runes) == 0 {
		return nil
	}
	if len(runes) > t.maxInputCharsPerWord {
		return []int32{t.unkID}
	}

	var out []int32
	start := 0
	for start < len(runes) {
		end := len(runes)
		var matchedID int32
		found := false
		for end > start {
			substr := string(runes[start:end])
			if start > 0 {
				substr = t.continuingSubwordPrefix + substr
			}
			if id, ok := t.vocab[substr]; ok {
				matchedID = id
				found = true
				break
			}
			end--
		}
		if !found {
			return []int32{t.unkID} // whole word -> UNK, discard any partial matches
		}
		out = append(out, matchedID)
		start = end
	}
	return out
}
