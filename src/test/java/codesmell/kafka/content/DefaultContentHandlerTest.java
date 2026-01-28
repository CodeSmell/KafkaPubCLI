package codesmell.kafka.content;

import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultContentHandlerTest {

    DefaultKafkaContentHandler contentHandler;

    @BeforeEach
    public void init() {
        contentHandler = new DefaultKafkaContentHandler();
    }

    @Test
    void test_processContents_null() {
        String topic = null;
        String content = null;
        var record = contentHandler.processContent(topic, content);
        assertNull(record);
    }

    @Test
    void test_processContents_empty() {
        String topic = "";
        String content = "";
        var record = contentHandler.processContent(topic, content);
        assertNull(record);
    }

    @Test
    void test_processContent_OnlyBody() {
        String topic = "theTopic";
        String content = """
                foo
                bar""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals(null, record.key());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
        assertEquals(content, record.value());
    }

    @Test
    void test_processContent_OnlyBody_unicode() {
        String topic = "theTopic";
        String content = """
                ümlaut""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals(null, record.key());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
        assertEquals(content, record.value());
    }

    @Test
    void test_processContent_withKey() {
        String topic = "theTopic";
        String content = """
                boo
                --key
                foo""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals("boo", record.key());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
        assertEquals("foo", record.value());                
    }

    @Test
    void test_processContent_withEmptyKey() {
        String topic = "theTopic";
        String content = """
                --key
                foo""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals(null, record.key());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
        assertEquals("foo", record.value());                
    }

    @Test
    void test_processContent_withMultiKeyBlocks() {
        String topic = "theTopic";
        String content = """
                boo
                --key
                hoo
                --key                
                foo""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals("boo", record.key());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
        assertEquals("""
                hoo
                --key                
                foo""", record.value());                  
    } 

    @Test
    void test_processContent_withHeaders() {
        String topic = "theTopic";
        String content = """
                header1:value1
                header2:value2
                --header
                foo""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals(null, record.key());
        assertEquals("foo", record.value());

        // Verify headers order and values
        java.util.List<Header> headers = new java.util.ArrayList<>();
        record.headers().forEach(headers::add);
        assertNotNull(headers);
        assertEquals(2, headers.size());
        assertEquals("header1", headers.get(0).key());
        assertEquals("value1", new String(headers.get(0).value()));
        assertEquals("header2", headers.get(1).key());
        assertEquals("value2", new String(headers.get(1).value()));
    }

    @Test
    void test_processContent_withEmptyHeaders() {
        String topic = "theTopic";
        String content = """
                --header
                foo""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals(null, record.key());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());        
        assertEquals("foo", record.value());
    }

    @Test
    void test_processContent_withBadHeaders() {
        String topic = "theTopic";
        String content = """
                boo
                --header
                foo""";
        
        assertThrows(IllegalArgumentException.class, () -> {
            contentHandler.processContent(topic, content);
        });
    }

    @Test
    void test_processContent_withKeyAndHeaders() {
        String topic = "theTopic";
        String content = """
                keyValue
                --key
                h1:v1
                h2:v2
                --header
                foo
                bar""";

        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals("keyValue", record.key());
        assertEquals("""
                foo
                bar""", record.value());

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

    @Test
    void test_processContent_bodyContainsBoundary_withKeyAndHeader() {
        String topic = "theTopic";
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


        var record = contentHandler.processContent(topic, content);
        assertNotNull(record);
        assertEquals(topic, record.topic());
        assertEquals("keyValue", record.key());
        assertEquals("""
                body line
                contains
                --header
                and
                --key
                markers
                end""", record.value());

        // Verify headers order and values
        java.util.List<Header> headers = new java.util.ArrayList<>();
        record.headers().forEach(headers::add);
        assertNotNull(headers);
        assertEquals(1, headers.size());
        assertEquals("h", headers.get(0).key());
        assertEquals("value", new String(headers.get(0).value()));
    }
}