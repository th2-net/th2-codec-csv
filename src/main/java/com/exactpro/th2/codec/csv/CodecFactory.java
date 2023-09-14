/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.csv;

import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecContext;
import com.exactpro.th2.codec.api.IPipelineCodecFactory;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.google.auto.service.AutoService;

@AutoService(IPipelineCodecFactory.class)
public class CodecFactory implements IPipelineCodecFactory {
    private static final Set<String> PROTOCOLS = Collections.singleton("csv");

    @NotNull
    @Override
    public Set<String> getProtocols() {
        return PROTOCOLS;
    }

    @NotNull
    @Override
    public Class<? extends IPipelineCodecSettings> getSettingsClass() {
        return CsvCodecConfiguration.class;
    }

    @NotNull
    @Override
    public IPipelineCodec create(@Nullable IPipelineCodecSettings settings) {
        if (settings instanceof CsvCodecConfiguration) {
            return new CsvCodec((CsvCodecConfiguration)settings);
        }
        throw new IllegalArgumentException("Unexpected setting type: " + (settings == null ? "null" : settings.getClass()));
    }

    @Override
    public void init(@NotNull IPipelineCodecContext iPipelineCodecContext) {
    }

    @Override
    public void init(@NotNull InputStream inputStream) {
    }

    @Override
    public void close() {
    }
}