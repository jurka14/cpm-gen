package cpm;

import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Query {

    private final Map<String, Document> documentMap;
    private final Map<String, String> connectorMap;
    private final ProvFactory pf = InteropFramework.getDefaultFactory();

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

        List<String> equals = ParquetComparator.getEquals(dataLists);

        boolean disjunct = equals.isEmpty();

        System.out.println("The training and evaluation datasets " + (disjunct ? "ARE" : "ARE NOT") + " disjunct!");

        if (!disjunct) {
            System.out.println("Files present in both datasets:");

            for (String s : equals) {
                System.out.println(s);
            }
        }
    }

    /**
     * Recursively traverses the prov documents through backward connectors to find the tiling processes.
     * @return List of lists of parquet files to be checked for disjunction.
     */
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
