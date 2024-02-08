package cpm;

import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.*;
import org.openprovenance.prov.scala.interop.FileInput;
import org.openprovenance.prov.scala.nf.*;

import java.io.File;

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
}