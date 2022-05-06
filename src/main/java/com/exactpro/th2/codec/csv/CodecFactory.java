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

public class CodecFactory implements IPipelineCodecFactory {
    public static final String PROTOCOL = "csv";
    private static final Set<String> PROTOCOLS = Collections.singleton(PROTOCOL);

    @NotNull
    @Override
    public String getProtocol() {
        return PROTOCOL;
    }

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
