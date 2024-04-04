package cpm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Bundle;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Identifiable;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.model.Statement;
import org.openprovenance.prov.scala.interop.FileInput;
import org.openprovenance.prov.scala.nf.*;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CpmFactory  {

    private ProvFactory pf;
    private InteropFramework intF;

    public CpmFactory() {
        pf = InteropFramework.getDefaultFactory();
        intF = new InteropFramework();
    }

    private Document canonize(String filePath) {
        org.openprovenance.prov.scala.immutable.Document nDoc = CommandLine$.MODULE$
                .parseDocumentToNormalForm(new FileInput(new File(filePath))).toDocument();

        DocumentProxyFromStatements fDoc = Normalizer$.MODULE$.
                fusion(nDoc);

        return new DocumentProxy(
                new NoIdStatementIndexer(fDoc.getStatements()),
                new StatementIndexer(),
                fDoc.getNamespace()
        ).toDocument();
    }

    private Document createBackbone(String bindingsFilePath) throws FileNotFoundException {
        Expand expand = new Expand(pf, false, false);

        Bindings bind;

        try {
            bind = new ObjectMapper().readValue(new File(bindingsFilePath), Bindings.class);
        } catch (IOException e) {
            throw new FileNotFoundException("Bindings file for the backbone was not found.");
        }

        return expand.expander(intF.readDocumentFromFile("backbone_tmpl.provn"), bind);
    }

    private void createPids(Document backboneDoc) {
        Iterator<Statement> it = ((Bundle) backboneDoc.getStatementOrBundle().get(0)).getStatement().iterator();

        while(it.hasNext()) {
            Statement s = it.next();
            List<Class<?>> interfaces = Arrays.asList(s.getClass().getInterfaces());
            if (interfaces.contains(Identifiable.class)) {
                String prefix = ((Identifiable) s).getId().getPrefix();
                if (prefix == "doi") { //TODO move this dependency to the pid generator
                    // create doi
                }
            }

        }

    }
}