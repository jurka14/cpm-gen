package cpm;

import com.fasterxml.jackson.databind.ObjectMapper;
import cpm.pid.PidGenerator;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Bundle;
import org.openprovenance.prov.model.HasType;
import org.openprovenance.prov.model.Identifiable;
import org.openprovenance.prov.model.Statement;
import org.openprovenance.prov.model.*;
import org.openprovenance.prov.scala.interop.StreamInput;
import org.openprovenance.prov.scala.nf.*;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CpmGenerator {

    private static final String BB_TEMPLATE = "backbone_tmpl.provn";
    private static final String BB_TEMPLATE_FIRST = "backbone_tmpl_first.provn";
    private final ProvFactory pf;
    private final InteropFramework intF;

    public CpmGenerator() {
        pf = InteropFramework.getDefaultFactory();
        intF = new InteropFramework();
    }

    public Document createBundle(String backboneBindingsPath, Document dsDoc, PidGenerator pidGen, boolean first) throws IOException {
        Document bbDoc = createBackbone(backboneBindingsPath, first);

        createPids(bbDoc, pidGen);

        IndexedDocument iDoc = new IndexedDocument(pf, bbDoc, false);
        iDoc.merge(dsDoc);

        Document finalDoc = iDoc.toDocument();

        canonize(finalDoc);

        return finalDoc;
    }


    private void canonize(Document doc) {

        //all the statements from the bundle in the document must be taken out to a blank document
        Bundle b = (Bundle) doc.getStatementOrBundle().get(0);
        Document docToCanonize = pf.newDocument();

        docToCanonize.getStatementOrBundle().addAll(b.getStatement());
        docToCanonize.setNamespace(doc.getNamespace());

        b.getStatement().clear();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        intF.writeDocument(baos, docToCanonize, Formats.ProvFormat.PROVN);

        org.openprovenance.prov.scala.immutable.Document nDoc = CommandLine$.MODULE$
                .parseDocumentToNormalForm(new StreamInput(new ByteArrayInputStream(baos.toByteArray()))).toDocument();

        DocumentProxyFromStatements fDoc = Normalizer$.MODULE$.
                fusion(nDoc);

        org.openprovenance.prov.scala.immutable.Document canonizedScalaDoc = new DocumentProxy(
                new NoIdStatementIndexer(fDoc.getStatements()),
                new StatementIndexer(),
                fDoc.getNamespace()
        ).toDocument();

        //convert to java representation
        BeanTraversal bt = new BeanTraversal(pf, pf);
        Document canonizedDoc = bt.doAction(canonizedScalaDoc);

        canonizedDoc.getStatementOrBundle();

        for (StatementOrBundle s : canonizedDoc.getStatementOrBundle()) {
            b.getStatement().add((Statement) s);
        }
    }

    private Document createBackbone(String bindingsFilePath, boolean first) throws FileNotFoundException {
        Expand expand = new Expand(pf, false, false);

        Bindings bind;

        try {
            bind = new ObjectMapper().readValue(new File(bindingsFilePath), Bindings.class);
        } catch (IOException e) {
            throw new FileNotFoundException("Bindings file for the backbone was not found.");
        }


        return expand.expander(intF.readDocumentFromFile(first ? BB_TEMPLATE_FIRST : BB_TEMPLATE), bind);
    }

    private void createPids(Document backboneDoc, PidGenerator pidGen) {
        //check if the generator does something
        if (pidGen.getNamespace() == null) {
            return;
        }

        Bundle b = (Bundle) backboneDoc.getStatementOrBundle().get(0);
        String bundleId = b.getId().getPrefix() + ":" + b.getId().getLocalPart();

        for (Statement s : b.getStatement()) {
            List<Class<?>> interfaces = Arrays.asList(s.getClass().getInterfaces());

            if (interfaces.contains(Identifiable.class) && interfaces.contains(HasType.class)) {
                QualifiedName id = ((Identifiable) s).getId();

                if (Objects.equals(id.getPrefix(), pidGen.getNamespace())) {
                    String type = ((HasType) s).getType().get(0).getType().getLocalPart();
                    String name = bundleId + "-" + type;

                    pidGen.generate(id.getLocalPart(), name);
                }
            }
        }
    }
}