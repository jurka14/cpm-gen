package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.*;
import org.openprovenance.prov.model.Agent;
import org.openprovenance.prov.model.Entity;
import org.openprovenance.prov.model.WasAttributedTo;
import org.openprovenance.prov.model.WasDerivedFrom;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Little {


    public static final String PROVBOOK_NS = "http://www.provbook.org";
    public static final String PROVBOOK_PREFIX = "provbook";

    public static final String JIM_PREFIX = "jim";
    public static final String JIM_NS = "http://www.cs.rpi.edu/~hendler/";

    private final ProvFactory pFactory;
    private final Namespace ns;
    public Little(ProvFactory pFactory) {
        this.pFactory = pFactory;
        ns=new Namespace();
        ns.addKnownNamespaces();
        ns.register(PROVBOOK_PREFIX, PROVBOOK_NS);
        ns.register(JIM_PREFIX, JIM_NS);
    }

    public QualifiedName qn(String n) {
        return ns.qualifiedName(PROVBOOK_PREFIX, n, pFactory);
    }

    public Document makeDocument() {
        Entity quote = pFactory.newEntity(qn("a-little-provenance-goes-a-long-way"));
        quote.setValue(pFactory.newValue("A little provenance goes a long way",
                pFactory.getName().XSD_STRING));

        Entity original = pFactory.newEntity(ns.qualifiedName(JIM_PREFIX,"LittleSemanticsWeb.html",pFactory));

        Agent paul = pFactory.newAgent(qn("Paul"), "Paul Groth");
        Agent luc = pFactory.newAgent(qn("Luc"), "Luc Moreau");

        WasAttributedTo attr1 = pFactory.newWasAttributedTo(null,
                quote.getId(),
                paul.getId());
        WasAttributedTo attr2 = pFactory.newWasAttributedTo(null,
                quote.getId(),
                luc.getId());

        WasDerivedFrom wdf = pFactory.newWasDerivedFrom(quote.getId(),
                original.getId());

        Document document = pFactory.newDocument();
        document.getStatementOrBundle()
                .addAll(Arrays.asList(new StatementOrBundle[] { quote,
                        paul,
                        luc,
                        attr1,
                        attr2,
                        original,
                        wdf }));
        document.setNamespace(ns);
        return document;
    }

    public void doConversions(Document document, String file) {
        InteropFramework intF=new InteropFramework();
        intF.writeDocument(file, document);
        //intF.writeDocument(System.out, Formats.ProvFormat.PROVN, document);
    }

    public void closingBanner() {
        System.out.println("");
        System.out.println("*************************");
    }

    public void openingBanner() {
        System.out.println("*************************");
        System.out.println("* Converting document  ");
        System.out.println("*************************");
    }

    public static void main(String [] args) {

        /*
        ProvFactory pf = InteropFramework.getDefaultFactory();
        InteropFramework intF = new InteropFramework();

        Little little=new Little(pf);
        little.openingBanner();
        Document document = intF.readDocumentFromFile("eval.provn");
        little.doConversions(document, "eval.svg");
        little.closingBanner();


         */




        ProvFactory pf = InteropFramework.getDefaultFactory();

        Little little=new Little(pf);

        Expand expand = new Expand(pf, false, false);

        InteropFramework intF = new InteropFramework();

        Bindings bind;

        try {
            bind = new ObjectMapper().readValue(new File("bindings_bb.json"), Bindings.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Document doc = expand.expander(intF.readDocumentFromFile("backbone_tmpl.provn"), bind);

        intF.writeDocument(System.out, doc, Formats.ProvFormat.PROVN);
        little.doConversions(doc, "backbone.svg");


        little.closingBanner();

        //new DoiGenerator().generate("lol", "Test2", "https://lmaoo.lol/");

/*
        Document doc = CommandLine$.MODULE$.
                parseDocumentToNormalForm(new FileInput(new File("test.provn"))).toDocument();

        DocumentProxyFromStatements newdoc = Normalizer$.MODULE$.
                fusion((org.openprovenance.prov.scala.immutable.Document) doc);

        System.out.println(newdoc);

        var ni = new NoIdStatementIndexer(newdoc.getStatements());
        var i = new StatementIndexer();
        var o = new DocumentProxy(ni, i, newdoc.getNamespace());


        InteropFramework intF=new InteropFramework();
        intF.writeDocument(System.out, o.toDocument(), Formats.ProvFormat.PROVN);
        intF.writeDocument(System.out, doc, Formats.ProvFormat.PROVN);


*/

/*
        String file="little.svg";

        ProvFactory pf = InteropFramework.getDefaultFactory();

        Little little=new Little(pf);
        little.openingBanner();
        Document document = little.makeDocument();
        little.doConversions(document, file);
        little.closingBanner();





        little.closingBanner();

        Expand expand = new Expand(pf, false, false);

        InteropFramework intF = new InteropFramework();

        Bindings bind;

        try {
            bind = new ObjectMapper().readValue(new File("bindings.json"), Bindings.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Document doc = expand.expander(intF.readDocumentFromFile("template.provn"), bind);


        little.doConversions(doc, "template.svg");

        little.closingBanner();

        Entity quote = pf.newEntity(little.qn("a-little-provenance-goes-a-long-fucking-way"));
        quote.setValue(pf.newValue("A little provenance goes a long fucking way",
                pf.getName().XSD_STRING));

        SpecializationOf spec = pf.newSpecializationOf(
                quote.getId(),
                little.qn("a-little-provenance-goes-a-long-way")
        );

        Document merge = pf.newDocument();
        merge.getStatementOrBundle()
                .addAll(Arrays.asList(quote, spec));
        merge.setNamespace(little.ns);

        little.doConversions(merge, "merge.svg");
        little.closingBanner();


        IndexedDocument iDoc = new IndexedDocument(pf,
                pf.newDocument(),
                true);

        iDoc.merge(document);
        iDoc.merge(merge);

        little.doConversions(iDoc.toDocument(), "merged.svg");
        little.closingBanner();
*/

    }

}