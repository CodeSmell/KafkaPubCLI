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
    void test_splitContentIntoParts_noHeaders() {
        String content = "foo\r\nbar";
        
        KafkaParts parts = contentHandler.splitContentIntoParts(content);
        
        assertNotNull(parts);
        assertEquals(null, parts.key);
        assertEquals(null, parts.headers);
        assertEquals(content, parts.body);
    }
    
    @Test
    void test_splitContentIntoParts_withHeaders() {
        String content = "foo\r\nbar";
        String fullContent = this.contentWithHeader(content); 
        
        KafkaParts parts = contentHandler.splitContentIntoParts(fullContent);
        
        assertNotNull(parts);
        assertEquals(null, parts.key);
        
        List<Header> headers = parts.headers;
        assertNotNull(headers);
        assertEquals(2, headers.size());
        
        assertEquals("key", headers.get(0).key());
        assertEquals("value", new String(headers.get(0).value()));
        
        assertEquals("key2", headers.get(1).key());
        assertEquals("value2", new String(headers.get(1).value()));
        
        assertEquals(content, parts.body);
    }
    
    private String contentWithHeader(String content) {
        StringBuilder sb = new StringBuilder();
        sb.append("key:value").append("\r\n");
        sb.append("key2:value2").append("\r\n");
        sb.append(DefaultContentHandler.HEADER_BOUNDARY).append("\r\n");
        sb.append(content);
        
        return sb.toString();
    }

}
