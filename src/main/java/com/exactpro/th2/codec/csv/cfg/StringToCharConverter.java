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

import com.fasterxml.jackson.databind.util.StdConverter;

public class StringToCharConverter extends StdConverter<String, Character> {
    @Override
    public Character convert(String value) {
        if (value == null) {
            throw new IllegalArgumentException("'null' cannot be converted to char");
        }
        if (value.length() > 1) {
            throw new IllegalArgumentException("String value " + value + " contains more than one character: " + value.length());
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("String value must not be empty to convert to char");
        }
        return value.charAt(0);
    }
}
