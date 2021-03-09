/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class TestCsvCodecConfiguration {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void deserializeTabAsDelimiter() throws JsonProcessingException {
        var value = "{\n"
                + "  \"delimiter\": \"\\t\"\n"
                + "}";
        CsvCodecConfiguration cfg = MAPPER.readValue(value, CsvCodecConfiguration.class);
        assertEquals('\t', cfg.getDelimiter());
    }
}