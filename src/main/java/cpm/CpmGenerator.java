package cpm;

import com.fasterxml.jackson.databind.ObjectMapper;
import cpm.pid.PidGenerator;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Bundle;
import org.openprovenance.prov.model.HasType;
import org.openprovenance.prov.model.Identifiable;
import org.openprovenance.prov.model.SpecializationOf;
import org.openprovenance.prov.model.Statement;
import org.openprovenance.prov.model.*;
import org.openprovenance.prov.scala.interop.StreamInput;
import org.openprovenance.prov.scala.nf.*;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;
import org.openprovenance.prov.template.json.Descriptor;
import org.openprovenance.prov.template.json.Descriptors;
import org.openprovenance.prov.template.json.QDescriptor;

import java.io.*;
import java.util.ArrayList;
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

        Bindings backboneBindings;

        try {
            backboneBindings = new ObjectMapper().readValue(new File(backboneBindingsPath), Bindings.class);
        } catch (IOException e) {
            throw new FileNotFoundException("Bindings file for the backbone was not found.");
        }

        long timestamp = System.currentTimeMillis();
        List<String> connIds = uniqueConnectorIds(backboneBindings, timestamp);

        Document bbDoc = createBackbone(backboneBindings, first);

        createPids(bbDoc, pidGen);

        IndexedDocument iDoc = new IndexedDocument(pf, bbDoc, false);
        iDoc.merge(dsDoc);

        Document finalDoc = iDoc.toDocument();

        editSpecializations(finalDoc, connIds, timestamp);

        canonize(finalDoc);

        return finalDoc;
    }

    /**
     * Makes backbone connector ids unique by adding a timestamp to them.
     * @return List of original connector ids.
     * */
    private List<String> uniqueConnectorIds(Bindings backboneBindings, long timestamp) {
        Descriptors backD = backboneBindings.var.get("back_conn_id");
        Descriptors forwardD = backboneBindings.var.get("forward_conn_id");

        List<String> connIds = new ArrayList<>();

        if (backD != null) {
            for (Descriptor id : backD.values) {
                connIds.add(((QDescriptor) id).id);
                ((QDescriptor) id).id = ((QDescriptor) id).id + timestamp;
            }
        }

        if (forwardD != null) {
            for (Descriptor id : forwardD.values) {
                connIds.add(((QDescriptor) id).id);
                ((QDescriptor) id).id = ((QDescriptor) id).id + timestamp;
            }
        }

        return connIds;
    }


    /** Edits the specializations in the domain-specific provenance to be compatible with the unique connector ids. */
    private void editSpecializations(Document doc, List<String> connIds, long timestamp) {

        Bundle b = (Bundle) doc.getStatementOrBundle().get(0);
        for (Statement s : b.getStatement()) {

            if (s.getKind() == StatementOrBundle.Kind.PROV_SPECIALIZATION) {

                QualifiedName id = ((SpecializationOf) s).getGeneralEntity();

                if (connIds.contains(id.getPrefix() + ":" + id.getLocalPart())) {
                    QualifiedName newId = pf.newQualifiedName(id.getNamespaceURI(), id.getLocalPart() + timestamp, id.getPrefix());
                    ((SpecializationOf) s).setGeneralEntity(newId);
                }
            }
        }
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

    private Document createBackbone(Bindings bind, boolean first) {

        Expand expand = new Expand(pf, false, false);

        return expand.expander(intF.readDocumentFromFile(first ? BB_TEMPLATE_FIRST : BB_TEMPLATE), bind);
    }

    /** Generates PIDs for backbone connectors based on the namespace prefix. */
    private void createPids(Document bbDoc, PidGenerator pidGen) {
        //check if the generator does something
        if (pidGen.getNamespace() == null) {
            return;
        }

        Bundle b = (Bundle) bbDoc.getStatementOrBundle().get(0);
        String bundleId = b.getId().getPrefix() + ":" + b.getId().getLocalPart();

        for (Statement s : b.getStatement()) {

            if (s.getKind() == StatementOrBundle.Kind.PROV_ENTITY) {
                QualifiedName id = ((Identifiable) s).getId();

                if (Objects.nonNull(id) && Objects.equals(id.getPrefix(), pidGen.getNamespace())) {
                    String type = ((QualifiedName)((HasType) s).getType().get(0).getValue()).getLocalPart();
                    String name = bundleId + "-" + type;

                    pidGen.generate(id.getLocalPart(), name);
                }
            }
        }
    }
}