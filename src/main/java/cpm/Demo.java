package cpm;

import com.exasol.parquetio.data.Row;
import com.exasol.parquetio.reader.RowParquetReader;
import cpm.mlflow.MLFlowGenerator;
import cpm.pid.DummyPidGenerator;
import cpm.pid.PidGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class Demo {
    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";

    private static void generateProvenance() {

        CpmGenerator gen = new CpmGenerator();
        MLFlowGenerator mlfGen = new MLFlowGenerator(TRACKING_URI);
        PidGenerator pidGen = new DummyPidGenerator();

        generateBundle(gen, mlfGen, pidGen, "preprocEval", true);
        generateBundle(gen, mlfGen, pidGen, "preprocTrain", true);
        generateBundle(gen, mlfGen, pidGen, "train", false);
        generateBundle(gen, mlfGen, pidGen, "eval", false);
    }

    private static void generateBundle(CpmGenerator gen, MLFlowGenerator mlfGen, PidGenerator pidGen, String name, boolean first) {

        name = name + "/" + name;

        Document dsDoc = mlfGen.generate(name + "_config.json");
        Document doc;

        try {
            doc = gen.createBundle(name + "_bindings_bb.json", dsDoc, pidGen, first);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        InteropFramework intF = new InteropFramework();
        intF.writeDocument(name + ".provn", doc);
        intF.writeDocument(name + ".svg", doc);
    }

    public static void main(String [] args) {

        generateProvenance();

    }

    private static void query() {

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

    /*
        //convert yaml to json
        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        Object obj = yamlReader.readValue(new FileInputStream(file), Object.class);

        ObjectMapper jsonWriter = new ObjectMapper();
        return jsonWriter.writeValueAsString(obj);
     */

}