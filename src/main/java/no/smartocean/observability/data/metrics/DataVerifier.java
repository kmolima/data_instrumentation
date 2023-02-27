package no.smartocean.observability.data.metrics;

import  no.smartocean.exceptions.IntegrityException;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DataVerifier {

    /**
     * Verifies if path in @param f is a file and a valid Aanderaa's XML according to the schema
     * @param path - path to the file be checked
     */
    public void verifyAADIXMLFile(Path path) throws IntegrityException {

        if(!Files.isRegularFile(path))
            throw new IntegrityException("Failed file check");

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

        try {
            Schema schema = factory.newSchema(getClass().getResource("/schemas/RTOutSchema.xsd"));
            Validator validator = schema.newValidator();
            validator.validate(new StreamSource(path.toFile()));
        } catch (SAXException | IOException e) {
            e.printStackTrace();
            throw new IntegrityException("Failed schema check: "+e.getCause());
        }
    }

    /**
     * Verifies if path in @param f is a file and a valid Aanderaa's XML according to the schema
     * @param data - data be checked
     */
    public void verifyAADIXMLStr(ByteArrayInputStream data) throws IntegrityException {

        if(data.available() <= 0 )
            return;

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

        try {
            Schema schema = factory.newSchema(getClass().getResource("/schemas/RTOutSchema.xsd"));
            Validator validator = schema.newValidator();
            Source xml = new StreamSource(data);
            validator.validate(xml);
        } catch (SAXException | IOException e) {
            e.printStackTrace();
            throw new IntegrityException("Failed schema check: "+e.getCause());
        }
    }
}
