package speech

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"notifier/libs/logging"
	"regexp"
	"time"

	"encoding/base64"
	"github.com/pkg/errors"
)

const (
	recognitionMaxAlternatives = 20
	googleRecognitionURL       = "https://speech.googleapis.com/v1/speech:recognize"
)

var (
	gLogger     = logging.WithPackage("speech_recognition")
	wordsRegexp = regexp.MustCompile(`(["='|/<>\\;:.,\s!?]+)`)
)

type Audio struct {
	Content    []byte
	Encoding   string
	SampleRate int
}

type Recognizer interface {
	TextFromAudio(ctx context.Context, file *Audio, lang string, speechContexts ...[]string) (string, error)
	WordsFromAudio(ctx context.Context, file *Audio, lang string, speechContexts ...[]string) ([]string, error)
}

type GoogleRecognizer struct {
	apiKey     string
	httpClient *http.Client
}

func NewGoogleRecognizer(apiKey string, timeout int) *GoogleRecognizer {
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}
	return &GoogleRecognizer{httpClient: client, apiKey: apiKey}
}

func (gr *GoogleRecognizer) TextFromAudio(ctx context.Context, file *Audio, lang string, speechContexts ...[]string) (string, error) {
	alternatives, err := gr.sendAudio(ctx, file, lang, 1, speechContexts...)
	if err != nil {
		return "", err
	}
	return alternatives[0], nil
}

func (gr *GoogleRecognizer) WordsFromAudio(ctx context.Context, file *Audio, lang string, speechContexts ...[]string) (
	[]string, error) {

	alternatives, err := gr.sendAudio(ctx, file, lang, 1, speechContexts...)
	if err != nil {
		return nil, err
	}
	return uniqueWordsFromTexts(alternatives), nil
}

func (gr *GoogleRecognizer) sendAudio(ctx context.Context, file *Audio, lang string, alternativesNum int,
	speechContexts ...[]string) ([]string, error) {

	logger := logging.FromContextAndBase(ctx, gLogger)
	request := map[string]map[string]interface{}{
		"config": {
			"encoding":        file.Encoding,
			"sampleRateHertz": file.SampleRate,
			"languageCode":    lang,
			"maxAlternatives": alternativesNum,
		},
		"audio": {
			"content": base64.StdEncoding.EncodeToString(file.Content),
		},
	}
	var hints []string
	for _, speechContext := range speechContexts {
		hints = append(hints, speechContext...)
	}
	if len(hints) != 0 {
		request["config"]["speechContexts"] = []map[string][]string{{"phrases": hints}}
	}

	requestStr, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "request serialization failed")
	}
	req, err := http.NewRequest("POST", googleRecognitionURL, bytes.NewBuffer(requestStr))
	if err != nil {
		return nil, errors.Wrap(err, "cannot build recognition http request")
	}
	req.Header.Set("Content-Type", "application/json")
	params := req.URL.Query()
	params.Set("key", gr.apiKey)
	req.URL.RawQuery = params.Encode()

	logger.Infof("Call recognition API, request: %s", requestStr)
	resp, err := gr.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "api request failed")
	}
	defer resp.Body.Close()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read api response")
	}
	logger.Infof("API response: %s", respContent)

	data := &googleAPIResp{}
	err = json.Unmarshal(respContent, data)
	if err != nil {
		return nil, errors.Wrap(err, "api response unmarshaling failed")
	}

	return gr.processResponse(ctx, data)
}

func (gr *GoogleRecognizer) processResponse(ctx context.Context, data *googleAPIResp) ([]string, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	if len(data.Results) == 0 {
		return nil, errors.New("data without results")
	}
	if len(data.Results) > 1 {
		logger.Warnf("Data contains more than one result")
	}
	result := data.Results[0]
	var alternatives []string
	for _, item := range result.Alternatives {
		if item.Transcript != "" {
			alternatives = append(alternatives, item.Transcript)
		}
	}
	if len(alternatives) == 0 {
		return nil, errors.New("result without alternatives")
	}
	logger.Infof("recognized alternatives: %s", alternatives)
	return alternatives, nil
}

type googleAPIResp struct {
	Results []*struct {
		Alternatives []*struct {
			Transcript string `json:"transcript"`
		} `json:"alternatives"`
	} `json:"results"`
}

func uniqueWordsFromTexts(texts []string) []string {
	wordSet := map[string]bool{}
	for _, text := range texts {
		for _, word := range wordsRegexp.Split(text, -1) {
			wordSet[word] = true
		}
	}
	wordList := make([]string, 0, len(wordSet))
	for word := range wordSet {
		wordList = append(wordList, word)
	}
	return wordList
}

func UniqueWordsFromText(text string) []string {
	return uniqueWordsFromTexts([]string{text})
}
