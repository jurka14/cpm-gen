package cpm;

import com.exasol.parquetio.data.Row;
import com.exasol.parquetio.reader.RowParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Query {

    private  Map<String, Document> documentMap;
    private  Map<String, String> connectorMap;
    private InteropFramework intF = new InteropFramework();
    private ProvFactory pf = InteropFramework.getDefaultFactory();

    private static final String PROCESS = "https://example.org/tilesGeneration";

    public Query(Map<String, Document> documentMap, Map<String, String> connectorMap) {
        this.documentMap = documentMap;
        this.connectorMap = connectorMap;
    }




    public void run(String bundleId) {

        List<List<Path>> dataLists = findData(bundleId);

        if (dataLists.size() != 2) {
            System.out.println("Datasets not found!");
            return;
        }

        boolean disjunct = true;

        for (Path p : dataLists.get(0)) {
            if (!checkDisjunction(p, dataLists.get(1))) {
                disjunct = false;
            }
        }

        System.out.println("The training and evaluation datasets " + (disjunct ? "ARE" : "ARE NOT") + " disjunct!");
    }


    private boolean checkDisjunction(Path path, List<Path> pathList) {
        boolean disjunct = true;

        for (Path p : pathList) {
            if (!checkFiles(path, p)) {
                disjunct = false;
            }
        }

        return disjunct;
    }

    private boolean checkFiles(Path path1, Path path2) {
        return true;
    }


    private List<List<Path>> findData(String bundleId) {

        List<List<Path>> pathLists = new ArrayList<>();

        Document doc = getDoc(bundleId);
        Activity mainAct = getMainActivity(doc);

        IndexedDocument iDoc = new IndexedDocument(pf, doc, true);
        Other hasPart = getHasPart(mainAct);
        List<String> backwardConnectors = getBackwardConnectors(iDoc, mainAct);


        if (Objects.nonNull(hasPart) && isTargetDoc(hasPart)) {
            pathLists.add(getDataFiles(iDoc, (QualifiedName) hasPart.getValue()));
            return pathLists;
        }

        if (backwardConnectors.isEmpty()) {
            return pathLists;
        }

        for (String conn : backwardConnectors) {
            pathLists.addAll(findData(resolveConnector(conn)));
        }

        return pathLists;
    }

    private List<Path> getDataFiles(IndexedDocument doc, QualifiedName domainSpecificProcess) {

        List<Path> dataFiles = new ArrayList<>();

        for (Entity e : getWsiEntities(doc, domainSpecificProcess)) {

            for (Other o : e.getOther()) {
                if (Objects.equals(o.getElementName().getUri(), "https://example.org/data")) {
                    String data = ((String) o.getValue());

                    Path temp;

                    try {
                        temp = Files.createTempFile("temp", ".parquet");
                        Files.write(temp, Base64.getDecoder().decode(data));
                    } catch (IOException exception) {
                        throw new RuntimeException(exception);
                    }

                    dataFiles.add(temp);
                }
            }
        }

        return dataFiles;
    }

    private List<Entity> getWsiEntities(IndexedDocument doc, QualifiedName domainSpecificProcess) {

        List<Entity> entityList = new ArrayList<>();

        for (Used u : doc.getUsed(doc.getActivity(domainSpecificProcess))) {
            Entity e = doc.getEntity(u.getEntity());

            if (e.getId().getLocalPart().startsWith("WSIData")) {
                entityList.add(e);
            }
        }

        return entityList;
    }


    private void query() {

        InteropFramework intF = new InteropFramework();

        Document pqDoc = intF.readDocumentFromFile("preprocEval.provn");
        Bundle b = (Bundle) pqDoc.getStatementOrBundle().get(0);
        String data = null;

        for (Statement s : b.getStatement()) {
            if (s.getKind() == StatementOrBundle.Kind.PROV_ENTITY &&
                    (Objects.equals(((Identifiable) s).getId().getLocalPart(), "WSIDataEval"))
            ) {
                for (Other o : ((Entity) s).getOther()) {
                    if (Objects.equals(o.getElementName().getLocalPart(), "data")) {
                        data = (String) o.getValue();
                    }
                }

            }
        }

        Path temp;

        try {
            temp = Files.createTempFile("temp", ".parquet");
            Files.write(temp, Base64.getDecoder().decode(data));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (ParquetReader<Row> reader = RowParquetReader
                .builder(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(temp.toString()), new Configuration())).build()) {
            Row row = reader.read();

            System.out.println(row.getFieldNames());

            int i = 0;
            while (i < 100) {
                List<Object> values = row.getValues();
                System.out.println(values);

                row = reader.read();
                i++;
            }
        } catch (final IOException exception) {
            //
        }
    }

    private boolean isTargetDoc(Other hasPart) {

        return Objects.equals(((QualifiedName) hasPart.getValue()).getUri(), PROCESS);
    }

    private Other getHasPart(Activity act) {

        if (Objects.isNull(act)) {
            return null;
        }

        for (Other o : act.getOther()) {
            if (Objects.equals(o.getElementName().getUri(), "https://w3id.org/reproduceme#hasPart")) {
                return o;
            }
        }

        return null;
    }

    private Document getDoc(String id) {
        return documentMap.get(id);
    }

    private String resolveConnector(String id) {
        return connectorMap.get(id);
    }

    private Activity getMainActivity(Document doc) {

        Bundle b = (Bundle) doc.getStatementOrBundle().get(0);

        for (Statement s : b.getStatement()) {
            if (s.getKind() == StatementOrBundle.Kind.PROV_ACTIVITY && (HasType.class.isAssignableFrom(s.getClass()))) {

                for (Type t : ((HasType) s).getType()) {
                    if (QualifiedName.class.isAssignableFrom(t.getValue().getClass()) &&
                            Objects.equals(((QualifiedName) t.getValue()).getUri(), "https://commonprovenancemodel.org/ns/cpm/mainActivity")) {
                        return ((Activity) s);
                    }
                }
            }
        }

        return null;
    }

    private List<String> getBackwardConnectors(IndexedDocument doc, Activity mainAct) {

        ArrayList<String> list = new ArrayList<>();

        Collection<Used> usedCollection = doc.getUsed(mainAct);

        if (Objects.isNull(usedCollection)) {
            return Collections.emptyList();
        }

        for (Used u : usedCollection) {

            QualifiedName qn = u.getEntity();
            list.add(qn.getUri());
        }

        return list;
    }

}
