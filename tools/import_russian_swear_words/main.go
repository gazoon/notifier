package main

import (
	"context"
	"flag"
	"io/ioutil"
	"notifier/config"
	"notifier/core"
	"notifier/libs/logging"
	"notifier/libs/speech"
	"notifier/storage"
	"regexp"
	"strings"

	"notifier/libs/models"

	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("russian_import")
)

func getSiteContent() (string, error) {
	b, err := ioutil.ReadFile("tools/import_russian_swear_words/page_content.txt")
	if err != nil {
		return "", errors.Wrap(err, "file error")
	}
	return string(b), nil
}

func retrieveHighlightedParts(content string) []string {
	re := regexp.MustCompile("<p><b>([^<>/()]+)( \\([^()]+\\))?</b>")
	matchedPairs := re.FindAllStringSubmatch(content, -1)
	var highlighted []string
	for _, pair := range matchedPairs {
		if len(pair) < 2 {
			gLogger.Warnf("tags matched, but without inner part: %s", pair)
			continue
		}
		highlighted = append(highlighted, pair[1])
	}
	return highlighted
}

func leaveSingleWordTexts(texts []string) []string {
	var filtered []string
	for _, text := range texts {
		words := speech.UniqueWordsFromText(text)
		if len(words) != 1 {
			gLogger.Infof("Not single word text: '%s', skip", text)
			continue
		}
		filtered = append(filtered, words[0])
	}
	return filtered
}

func filterUndesirableWords(words []string) []string {
	var filtered []string
	for _, word := range words {
		if strings.HasSuffix(word, ".") {
			gLogger.Infof("Word with . at the end: '%s', skip")
			continue
		}
		filtered = append(filtered, word)
	}
	return filtered
}

func saveToStorage(db *storage.NeoStorage, words []string) int {
	ctx := context.Background()
	var count int
	for _, word := range words {
		word = models.ProcessWord(word)
		logger := gLogger.WithField("swear_word", word)
		logger.Debug("Save new word to the storage")
		err := db.CreateSwearWord(ctx, word)
		if err != nil {
			logger.Errorf("Cannot save word to the storage: %s", err)
			continue
		}
		count += 1
	}
	return count
}

func main() {
	var confPath string
	config.FromCmdArgs(&confPath)
	flag.Parse()

	core.Initialization(confPath)

	neoStorage, err := core.CreateNeoStorage()
	if err != nil {
		panic(err)
	}
	content, err := getSiteContent()
	if err != nil {
		gLogger.Panicf("Cannot get site content: %s", err)
	}
	gLogger.Info("Retrieve highlithed tokens")
	tokens := retrieveHighlightedParts(content)
	gLogger.Info("Filter tokens with more than one word")
	words := leaveSingleWordTexts(tokens)
	gLogger.Info("Filter specific undesirable words")
	words = filterUndesirableWords(words)
	gLogger.Infof("Save to the storage result words list: %s", words)
	wordsNum := saveToStorage(neoStorage, words)
	gLogger.Infof("Successfully saved %d words", wordsNum)
}
