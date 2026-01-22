package codesmell.kafka.content;

import codesmell.kafka.content.DefaultContentHandler.KafkaParts;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultContentHandlerTest {

    DefaultContentHandler contentHandler;

    @BeforeEach
    public void init() {
        contentHandler = new DefaultContentHandler();
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
}