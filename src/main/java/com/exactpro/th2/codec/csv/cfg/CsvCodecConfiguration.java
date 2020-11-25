/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.csv.cfg;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

public class CsvCodecConfiguration {
    @JsonProperty("default-header")
    @JsonPropertyDescription("The default header that will be used for parsing received batch if no header found in the batch")
    private List<String> defaultHeader;

    @JsonPropertyDescription("The delimiter to use for splitting input data")
    private char delimiter = ',';

    @JsonPropertyDescription("Encoding to use during data decoding")
    private String encoding = "UTF-8";

    public List<String> getDefaultHeader() {
        return defaultHeader;
    }

    public void setDefaultHeader(List<String> defaultHeader) {
        this.defaultHeader = defaultHeader;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
}
