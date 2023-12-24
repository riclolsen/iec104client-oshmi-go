package main

import (
	"bufio"
	"os"
	"strings"
)

func readINIFile(filename string) (map[string]map[string]string, error) {
	// Create a map of sections to key/value pairs
	iniData := make(map[string]map[string]string)

	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	scanner := bufio.NewScanner(file)

	var currentSection string

	// Read the file line by line
	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines and comments
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, ";") {
			continue
		}

		// Check for a new section
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			// Get the section name
			currentSection = strings.TrimSpace(line[1 : len(line)-1])

			// Create a new map for the section
			iniData[currentSection] = make(map[string]string)
		} else {
			// Split the line into a key/value pair
			parts := strings.SplitN(line, "=", 2)

			// Add the key/value pair to the current section
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				iniData[currentSection][key] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return iniData, nil
}
