package codesmell.kafka.content;

import codesmell.kafka.content.DefaultKafkaContentHandler.KafkaParts;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultContentHandlerTest {

    DefaultKafkaContentHandler contentHandler;

    @BeforeEach
    public void init() {
        contentHandler = new DefaultKafkaContentHandler();
    }

    @Test
    void test_splitContentIntoParts_null() {
        String content = null;
        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNull(parts);
    }

    @Test
    void test_splitContentIntoParts_empty() {
        String content = "";
        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNull(parts);
    }

    @Test
    void test_splitContentIntoParts_noKey_noHeaders() {
        String content = """
                foo
                bar""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals(null, parts.key);
        assertEquals(null, parts.headers);
        assertEquals(content, parts.body);
    }

        @Test
    void test_splitContentIntoParts_unicode_noKey_noHeaders() {
        String content = """
                ümlaut""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals(null, parts.key);
        assertEquals(null, parts.headers);
        assertEquals(content, parts.body);
    }

    @Test
    void test_splitContentIntoParts_withHeaders() {
        String content = """
                header1:value1
                header2:value2
                --header
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals(null, parts.key);
        List<Header> headers = parts.headers;
        assertNotNull(headers);
        assertEquals(2, headers.size());
        assertEquals("header1", headers.get(0).key());
        assertEquals("value1", new String(headers.get(0).value()));
        assertEquals("header2", headers.get(1).key());
        assertEquals("value2", new String(headers.get(1).value()));
        assertEquals("foo", parts.body);
    }

    @Test
    void test_splitContentIntoParts_withEmptyHeaders() {
        String content = """
                --header
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals(null, parts.key);
        List<Header> headers = parts.headers;
        assertNull(headers);
        assertEquals("foo", parts.body);
    }

    //@Test
    void test_splitContentIntoParts_withBadHeaders() {
        String content = """
                boo
                --header
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals(null, parts.key);
        List<Header> headers = parts.headers;
        assertNotNull(headers);
        assertEquals(2, headers.size());
        assertEquals("header1", headers.get(0).key());
        assertEquals("value1", new String(headers.get(0).value()));
        assertEquals("header2", headers.get(1).key());
        assertEquals("value2", new String(headers.get(1).value()));
        assertEquals("foo", parts.body);
    }

    @Test
    void test_splitContentIntoParts_withKey() {
        String content = """
                boo
                --key
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals("boo", parts.key);
        List<Header> headers = parts.headers;
        assertNull(headers);
        assertEquals("foo", parts.body);
    }

    @Test
    void test_splitContentIntoParts_withMultiKeyBlocks() {
        String content = """
                boo
                --key
                hoo
                --key                
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals("boo", parts.key);
        List<Header> headers = parts.headers;
        assertNull(headers);
                assertEquals("""
                hoo
                --key                
                foo""", parts.body);
    }

    @Test
    void test_splitContentIntoParts_withEmptyKey() {
        String content = """
                --key
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals("", parts.key);
        List<Header> headers = parts.headers;
        assertNull(headers);
        assertEquals("foo", parts.body);
    }

    @Test
    void test_splitContentIntoParts_withKeyAndHeaders() {
        String content = """
                boo
                --key
                header1:value1
                header2:value2
                --header
                foo""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals("boo", parts.key);
        List<Header> headers = parts.headers;
        assertNotNull(headers);
        assertEquals(2, headers.size());
        assertEquals("header1", headers.get(0).key());
        assertEquals("value1", new String(headers.get(0).value()));
        assertEquals("header2", headers.get(1).key());
        assertEquals("value2", new String(headers.get(1).value()));
        assertEquals("foo", parts.body);
    }

    @Test
    void test_splitContentIntoParts_bodyContainsBoundary_withKeyAndHeader() {
        String content = """
                keyValue
                --key
                h:value
                --header
                body line
                contains
                --header
                and
                --key
                markers
                end""";

        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        assertNotNull(parts);
        assertEquals("keyValue", parts.key);
        assertNotNull(parts.headers);
        assertEquals(1, parts.headers.size());
        assertEquals("h", parts.headers.get(0).key());
        assertEquals("value", new String(parts.headers.get(0).value()));
        assertEquals("""
                body line
                contains
                --header
                and
                --key
                markers
                end""", parts.body);
    }

    @Test
    void test_processContent() {
        String content = """
                keyValue
                --key
                h1:v1
                h2:v2
                --header
                theBody""";

        var record = contentHandler.processContent("theTopic", content);
        assertNotNull(record);
        assertEquals("theTopic", record.topic());
        assertEquals("keyValue", record.key());
        assertEquals("theBody", record.value());

        // Verify headers order and values
        java.util.List<Header> headers = new java.util.ArrayList<>();
        record.headers().forEach(headers::add);
        assertNotNull(headers);
        assertEquals(2, headers.size());
        assertEquals("h1", headers.get(0).key());
        assertEquals("v1", new String(headers.get(0).value()));
        assertEquals("h2", headers.get(1).key());
        assertEquals("v2", new String(headers.get(1).value()));
    }

}