package cpm;


import com.github.jasminb.jsonapi.JSONAPIDocument;
import org.gbif.datacite.rest.client.DataCiteClient;
import org.gbif.datacite.rest.client.configuration.ClientConfiguration;
import org.gbif.datacite.rest.client.exception.DataCiteClientException;
import org.gbif.datacite.rest.client.model.DoiSimplifiedModel;
import org.gbif.datacite.rest.client.retrofit.DataCiteRetrofitSyncClient;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.XMLOutputter;

import java.util.Base64;

public class DoiGenerator implements PidGenerator {

    private static final String NAMESPACE = "doi";
    private static final String PREFIX = "10.82851";
    private static final String APIURL = "https://api.test.datacite.org/";
    private static final String USER = "NOWA.SIVCDZ";
    private static final String PASS = "tCJi_qqPWSchb_4GSdA_UHDD";
    private static final String EVENT = "publish";



    private final DataCiteClient client;

    public DoiGenerator() {
        var config = ClientConfiguration.builder()
                .withBaseApiUrl(APIURL)
                .withUser(USER)
                .withPassword(PASS)
                .build();
        this.client = new DataCiteRetrofitSyncClient(config);
    }

    @Override
    public void generate(String pid, String name) {
        String doi = PREFIX + "/" + pid;
        var model = new DoiSimplifiedModel();
        model.setDoi(doi);
        model.setEvent(EVENT);
        //TODO create target url generation
        model.setUrl("https://test.url/");
        model.setXml(createXml(doi, name));
        var response = client.updateDoi(doi, new JSONAPIDocument<>(model));

        if (!response.isSuccessful()) {
            throw new DataCiteClientException(new Exception());
        }

    }

    @Override
    public String getNamespace() {
        return NAMESPACE;
    }

    private String createXml(String doi, String name) {
        Document doc = new Document();
        Namespace ns = Namespace.getNamespace("http://datacite.org/schema/kernel-4");
        Element root = new Element("resource", ns)
                .setAttribute(
                        "schemaLocation",
                        "http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4/metadata.xsd",
                        Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance"));
        Element identifier = new Element("identifier", ns)
                .addContent(doi)
                .setAttribute("identifierType", "DOI");
        Element creators = new Element("creators", ns);
        Element creator = new Element("creator", ns);
        Element creatorName = new Element("creatorName", ns).addContent("DP");
        Element titles = new Element("titles", ns);
        Element title = new Element("title", ns).addContent(name);
        Element  publisher = new Element("publisher", ns).addContent("DPs");
        Element  publicationYear = new Element("publicationYear", ns).addContent("2024");
        Element resourceType = new Element("resourceType", ns).setAttribute("resourceTypeGeneral", "Text");

        creator.addContent(creatorName);
        creators.addContent(creator);
        titles.addContent(title);

        root.addContent(identifier);
        root.addContent(creators);
        root.addContent(titles);
        root.addContent(publisher);
        root.addContent(publicationYear);
        root.addContent(resourceType);
        doc.setRootElement(root);

        String xml = new XMLOutputter().outputString(doc);

        return Base64.getEncoder().encodeToString(xml.getBytes());
    }

}
