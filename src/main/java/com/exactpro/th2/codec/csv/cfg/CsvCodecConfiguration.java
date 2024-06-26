/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CsvCodecConfiguration implements IPipelineCodecSettings {
    @JsonProperty("default-header")
    @JsonPropertyDescription("The default header that will be used for parsing received batch if no header found in the batch")
    private List<String> defaultHeader;

    @JsonPropertyDescription("The delimiter to use for splitting input data")
    @JsonDeserialize(converter = StringToCharConverter.class)
    private char delimiter = ',';

    @JsonPropertyDescription("Encoding to use during data decoding")
    private String encoding = StandardCharsets.UTF_8.name();

    @JsonProperty("display-name")
    @JsonPropertyDescription("Display name for the root event sent to the event store")
    private String displayName = "CodecCsv";

    @JsonProperty("validate-length")
    @JsonPropertyDescription("Set to validate length of columns or not")
    private boolean validateLength = true;

    @JsonProperty("publish-header")
    @JsonPropertyDescription("Set to enable header publication")
    private boolean publishHeader = false;

    @JsonProperty("trim-whitespace")
    @JsonPropertyDescription("Set to trim whitespace in header (when default-header isn't set) and cell")
    private boolean trimWhitespace = true;

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

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public boolean getValidateLength() {
        return validateLength;
    }

    public void setValidateLength(boolean  validateLength) {
        this.validateLength = validateLength;
    }

    public boolean isPublishHeader() {
        return publishHeader;
    }

    public void setPublishHeader(boolean publishHeader) {
        this.publishHeader = publishHeader;
    }

    public boolean isTrimWhitespace() {
        return trimWhitespace;
    }

    public void setTrimWhitespace(boolean trimWhitespace) {
        this.trimWhitespace = trimWhitespace;
    }
}
