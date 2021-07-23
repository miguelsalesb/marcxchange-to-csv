package main

import (
	"archive/zip"
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type libraryData struct {
	id                     string
	mattype                string
	isbn                   string
	legalDepositNumber     string
	languageOfWork         string
	originalLanguageOfWork string
	title                  string
	edition                string
	editor                 string
	pubdate                string
	pages                  string
	size                   string
	collection             string
	collection_vol         string
	subjects               string
	authors                string
	translator             string
	originalTitle          string
	image                  string
}

func main() {

	var (
		errLog, errFile                                 error
		logFile, file                                   *os.File
		fRecord, lRecord, nRecord                       string
		firstRecord, lastRecord, testCount1, testCount2 int
		fileOpened                                      bool
	)
	var counts = 0

	const ZIPFILE = "catalogo-bnp.zip"
	const FILE = "catalogo-bnp.csv"

	// Ask if a new file is to be created
	fmt.Print(`Do you want to create a new file? ("yes" to create / "no" to use a already created file)` + "\n")
	fmt.Scanln(&nRecord)

	if nRecord == "yes" {

		// Create the file to put the info
		file, errFile = os.Create("catalogo-bnp.csv")
		if errFile != nil {
			panic(errFile)
		}

	} else if nRecord == "no" {
		// Use the already created file
		file, errFile = os.OpenFile("catalogo-bnp.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if errFile != nil {
			panic(errFile)
		}
		fileOpened = true
	}

	// Check if file "catalogo-bnp.csv" exists
	if _, errFile = os.Stat("catalogo-bnp.csv"); errFile == nil {
		// Create the file to put the info
		file, errFile = os.OpenFile("catalogo-bnp.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if errFile != nil {
			panic(errFile)
		}
		fileOpened = true

	} else {
		// Use the already created file
		file, errFile = os.Create("catalogo-bnp.csv")
		if errFile != nil {
			panic(errFile)
		}
	}

	defer file.Close()

	file.Sync()

	var w = bufio.NewWriter(file)

	// Write the file headers
	// If the file was already created, don't insert the header
	if nRecord == "yes" || !fileOpened {
		w.WriteString("ID;Tipo de material;ISBN;Depósito legal;Língua da publicação;Língua da obra original;Título;Título original;Edição;Editor;Data de publicação;Extensão do item;Dimensões;Coleção;Volume;Temas;Imagem;Tradutor;Autores;\n")
	}

	// write the logs with the last scrapped record in the logs.txt file
	logFile, errLog = os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if errLog != nil {
		log.Println("LOG FILE ERROR: ", errLog)
	}
	defer logFile.Close()

	fmt.Print("Insert the number from the bibliographic repository where you want to start and then press Enter \n ")
	fmt.Scanln(&fRecord)
	firstRecord, _ = strconv.Atoi(fRecord)

	fmt.Print("Insert the number from the bibliographic repository where you want to finish and then press Enter \n ")
	fmt.Scanln(&lRecord)
	lastRecord, _ = strconv.Atoi(lRecord)

	for n := firstRecord; n <= lastRecord+1; n++ {

		url := fmt.Sprintf("%s%d", "http://urn.bn.pt/ncb/unimarc/marcxchange?id=", n)

		fmt.Println("Record: ", n)

		libData, c := getTitles(n, url)

		// Count the nonexistent records to finish the script when there are more than 50 nonexistent records
		if c {
			counts = counts + 1

		}
		if n == 2000000 {
			counts = 0
			testCount1 = n
		}

		if counts == 50 {
			testCount2 = n
		}

		if counts == 50 && testCount2-testCount1 > 50 {
			counts = 0
			testCount1 = n

		} else if counts == 50 && testCount2-testCount1 <= 50 && n >= 2000000 {

			fmt.Println("Finished scrapping the repository")
			// Zip file code retrieved from: https://golangcode.com/create-zip-files-in-go/
			// List of Files to Zip
			file := []string{"catalogo-bnp.csv"}

			// Zip the file
			if err := ZipFiles(ZIPFILE, file); err != nil {
				panic(err)
			}
			fmt.Println("Zipped File:", ZIPFILE)

			// Read the "catalogo-bnp.csv" file to get the last scrapped record
			lastScrappedRecord := readFile(FILE)

			log.SetOutput(logFile)

			// Write the last scrapped record in the log's file
			log.Printf("Last scrapped record: %s", lastScrappedRecord)

			os.Exit(0)
		} else if n == lastRecord+1 {

			file := []string{"catalogo-bnp.csv"}

			if err := ZipFiles(ZIPFILE, file); err != nil {
				panic(err)
			}

			log.SetOutput(logFile)

			// Write the last scrapped record in the log's file
			log.Printf("Last scrapped record: %s", lastRecord)

			os.Exit(0)

			// Stop the script when there are 50 nonexistent records in a row
		}
		WriteTitles(libData, w)
	}
	file.Close()

}

func readFile(fname string) string {

	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buf := make([]byte, 1000)
	stat, _ := os.Stat(fname)
	start := stat.Size() - 1000
	_, err = file.ReadAt(buf, start)
	if err != nil {
		fmt.Printf("Couldn't read the file in order to write the last scrapped record in the log's file")
	}

	lr := string(buf)

	r, _ := regexp.Compile("[0-9]{7};*")

	lastSR := r.FindAllString(lr, -1)
	lastScrappedRecord := lastSR[len(lastSR)-1]
	lastScrappedRecord = lastScrappedRecord[:len(lastScrappedRecord)-1]

	return lastScrappedRecord
}

func ZipFiles(filename string, files []string) error {

	newZipFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	// Add files to zip
	for _, file := range files {
		if err = AddFileToZip(zipWriter, file); err != nil {
			return err
		}
	}
	return nil
}

func AddFileToZip(zipWriter *zip.Writer, filename string) error {

	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	// Get the file information
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

func getTitles(n int, url string) (libraryData, bool) {

	var (
		idLibrary, leader, isbn, legalDepositNumber, matType, title, name, surname, translatorCode, translator, languageOfWork, originalLanguageOfWork,
		pubDate, origTi, originalTitle, edition, editor, pages, size, collection, collectionVol, subjs, subjects, authors, image string
		authors_array  = make([]string, 0, 2)
		subjects_array = []string{}
		count          bool
	)
	var replacerAuthor = strings.NewReplacer("<", "", ">", "", "«", "", "»", "", "º", "", "[", "", "]", "")
	var replacer = strings.NewReplacer("<", "", ">", "", ";", ",")
	const empty = ""

	res, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		fmt.Printf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the XML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		fmt.Println(err)
	}

	record_exists := doc.Find("controlfield")
	// fmt.Println("len(record_exists.Text()): ", len(record_exists.Text()))
	if len(record_exists.Text()) == 0 {
		count = true
		// fmt.Println("count: ", count)
	}

	doc.Find("datafield").Each(func(i int, s *goquery.Selection) {
		tag, _ := s.Attr("tag")
		ind1, _ := s.Attr("ind1")
		ind2, _ := s.Attr("ind2")

		leader = doc.Find("leader").Text()

		leader7 := leader[6:7]
		leader8 := leader[7:8]

		matType = empty

		// Get the material type
		switch {
		case ((leader7 == "a" || leader7 == "b") && leader8 == "s"):
			matType = "periódico"
		case (leader7 == "e" || leader7 == "f"):
			matType = "cartografia"
		case (leader7 == "k"):
			matType = "iconografia"
		case (leader7 == "a" || leader7 == "b"):
			matType = "livro"
		case (leader7 == "g" || leader7 == "m"):
			matType = "manuscrito"
		case (leader7 == "c" || leader7 == "d"):
			matType = "partitura"
		case (leader7 == "l"):
			matType = "recurso eletrónico"
		case (leader7 == "m"):
			matType = "multimédia"
		case (leader7 == "g"):
			matType = "vídeo"
		case (leader7 == "i" || leader7 == "j"):
			matType = "registo sonoro"
		}

		id := doc.Find("controlfield")
		if tag, ok := id.Attr("tag"); tag == "001" {
			if ok {
				idLibrary = id.First().Text()
			}
		}

		// Get the ISBN
		if tag == "010" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					isbn = e.Text()
				} else {
					isbn = empty
				}
			})
		}

		// Get the Legal Deposit Number
		if tag == "021" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "b" {
					legalDepositNumber = e.Text()
				} else {
					legalDepositNumber = empty
				}
			})
		}

		// Get the language and original language of the work
		if tag == "101" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					languageOfWork = e.Text()
				}
				if attr, _ := e.Attr("code"); attr == "c" {
					originalLanguageOfWork = e.Text()
				}
			})
		}

		// Get the title
		if tag == "200" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					ti := e.Text()
					title = replacer.Replace(ti)

				}
			})
		}

		// Get the edition
		if tag == "205" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					edition = e.Text()
				}
			})
		}

		// Get the editor
		if tag == "210" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "c" {
					ed := e.Text()
					editor = replacer.Replace(ed)
				}
			})
		}

		// Get the number of pages
		if tag == "215" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					pag := e.Text()
					pages = replacer.Replace((pag))
				}
			})
		}

		// Get the size
		if tag == "215" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "d" {
					size = e.Text()
				}
			})
		}

		// Get the title of the series and the item volume number within the series
		if tag == "225" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					co := e.Text()
					collection = replacer.Replace(co)
				}
				if attr, _ := e.Attr("code"); attr == "v" {
					co_v := e.Text()
					collectionVol = replacer.Replace(co_v)
				}
			})
		}

		// Get the original title
		if tag == "304" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					o := s.Text()
					origT := strings.Index(o, "orig.")
					if strings.Contains(o, ":") && len(o) > 12 {
						origTi = strings.TrimLeft(o[origT+6:], ":")
						origTi = strings.TrimLeft(origTi, " ")
					}
					// else {
					// 	origTi = strings.TrimLeft(o[origT+5:], ":")
					// 	origTi = strings.TrimLeft(origTi, " ")
					// }

					// replacer := strings.NewReplacer("<", "", ">", "", "'", "\\'", "«", "", "»", "", "º", "")
					originalTitle = replacer.Replace(origTi)
				}
			})
		}

		if ind1 == "4" && ind2 == "1" && tag == "856" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "u" {
					image = s.Text()
				}
			})
		}
	})

	// Get the publication date
	doc.Find("datafield").Each(func(i int, s *goquery.Selection) {

		tag, _ := s.Attr("tag")
		if tag == "210" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "d" {
					r, _ := regexp.Compile("[0-9]{4}")
					e := s.Text()
					dts := r.FindString(e)
					pubDate = dts
				}
			})
		}
	})

	// Get the Universal Decimal Classification
	doc.Find("datafield").Each(func(i int, s *goquery.Selection) {

		tag, _ := s.Attr("tag")
		if tag == "675" {
			s.Find("subfield").Each(func(i int, s *goquery.Selection) {
				if attr, _ := s.Attr("code"); attr == "a" {
					subjs = s.Text()
				}
			})
			if subjs != "" {
				subjects_array = append(subjects_array, subjs)
			}
		}

	})

	doc.Find("datafield").Each(func(i int, s *goquery.Selection) {

		tag, _ := s.Attr("tag")

		// Get the authors
		if tag[:1] == "7" {
			s.Find("subfield").Each(func(i int, e *goquery.Selection) {
				if attr, _ := e.Attr("code"); attr == "a" {
					nm := e.Text()
					name = replacerAuthor.Replace(nm)
				}
				if attr, _ := e.Attr("code"); attr == "b" {
					sn := e.Text()
					surname = replacerAuthor.Replace(sn)
				}
				if attr, _ := e.Attr("code"); attr == "4" {
					translatorCode = e.Text()
				}
			})

			if translatorCode == "730" {
				translator = name + ", " + surname
			}

			if name != "" && surname != "" {
				authors_array = append(authors_array, name+", "+surname)
			} else if name != "" && len(surname) == 0 {
				authors_array = append(authors_array, name)
			} else {
				authors_array = append(authors_array, empty)
			}
		}
	})

	if len(authors_array) > 0 {
		authors = strings.Join(authors_array, ";")
	}

	if len(subjects_array) > 0 {
		subjects = strings.Join(subjects_array, ";")
	}

	data := libraryData{
		idLibrary,
		matType,
		isbn,
		legalDepositNumber,
		languageOfWork,
		originalLanguageOfWork,
		title,
		edition,
		editor,
		pubDate,
		pages,
		size,
		collection,
		collectionVol,
		subjects,
		authors,
		translator,
		originalTitle,
		image,
	}
	return data, count
}

func WriteTitles(record libraryData, w *bufio.Writer) {

	idLibrary := record.id
	matType := record.mattype
	isbn := record.isbn
	legalDepositNumber := record.legalDepositNumber
	languageOfWork := record.languageOfWork
	originalLanguageOfWork := record.originalLanguageOfWork
	title := record.title
	edition := record.edition
	editor := record.editor
	pubDate := record.pubdate
	pages := record.pages
	size := record.size
	collection := record.collection
	collectionVol := record.collection_vol
	subjects := record.subjects
	authors := record.authors
	translator := record.translator
	originalTitle := record.originalTitle
	image := record.image

	// To avoid writing blanks where there are is no information to be scrapped due to the record being deleted
	if len(idLibrary) > 0 {
		w.WriteString(idLibrary + ";" + matType + ";" + isbn + ";" + legalDepositNumber + ";" + languageOfWork + ";" + originalLanguageOfWork + ";" + title + ";" + originalTitle + ";" + edition + ";" + editor + ";" + pubDate + ";" + pages + ";" + size + ";" + collection + ";" + collectionVol + ";" + subjects + ";" + image + ";" + translator + ";" + authors + "\n")
		w.Flush()
	}
}
