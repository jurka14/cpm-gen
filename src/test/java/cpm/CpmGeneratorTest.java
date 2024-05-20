package cpm;

import cpm.pid.PidGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Document;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CpmGeneratorTest {

    private Path backboneBindingsPath;
    PidGenerator pidGen;
    CpmGenerator cpmGen;
    InteropFramework intF = new InteropFramework();


    @BeforeEach
    void setUp() {
        try {
            backboneBindingsPath =  Files.createTempFile("bb", ".json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cpmGen = new CpmGenerator();
        pidGen = mock(PidGenerator.class);

    }
    @Test
    void createBundle() {
        when(pidGen.getNamespace()).thenReturn(null);
        createBbBindings();
        createBundle(false);
    }

    @Test
    void createFirstBundle() {
        when(pidGen.getNamespace()).thenReturn(null);
        createBbBindings();
        createBundle(true);
    }

    @Test
    void createBundleWithPids() {
        when(pidGen.getNamespace()).thenReturn("doi");
        createBbBindings();
        createBundle(false);
    }

    @Test
    void createBundleWithNoBindings() {
        assertThatThrownBy(() -> cpmGen.createBundle(backboneBindingsPath.toString(), getDsDoc(), pidGen, false, false))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("Bindings file for the backbone was not found.");
    }



    private void createBundle(boolean first) {
        Document dsDoc = getDsDoc();
        Document doc;

        try {
            doc = cpmGen.createBundle(backboneBindingsPath.toString(), dsDoc, pidGen, first, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertThat(doc).isNotNull();

        OutputStream os = new ByteArrayOutputStream();

        intF.writeDocument(os, doc, Formats.ProvFormat.PROVN);

        String actual = os.toString().replaceAll("\\r\\n?", "\n");
        String expected = (first ? getFirstDoc() : getDoc()).replaceAll("\\r\\n?", "\n");

        assertThat(actual).isEqualTo(expected);
    }

    private String getDoc() {
        return """
                document
                default <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix def <https://example.org/>
                prefix repr <https://w3id.org/reproduceme#>
                prefix pre_0 <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix doi <https://doi.org/10.58092/>
                prefix cpm <https://commonprovenancemodel.org/ns/cpm/>
                prefix bndl <http://_PLACEHOLER_:8000/api/v1/organizations/ORG/graphs/>
                prefix meta <http://_PLACEHOLER_:8000/api/v1/graphs/meta/>
                bundle bndl:train
                default <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix def <https://example.org/>
                prefix repr <https://w3id.org/reproduceme#>
                prefix pre_0 <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix doi <https://doi.org/10.58092/>
                prefix cpm <https://commonprovenancemodel.org/ns/cpm/>
                prefix bndl <http://_PLACEHOLER_:8000/api/v1/organizations/ORG/graphs/>
                prefix meta <http://_PLACEHOLER_:8000/api/v1/graphs/meta/>
                                
                entity(doi:trainedModelConnector,[prov:type = 'cpm:forwardConnector', cpm:senderBundleId = 'bndl:eval'])
                wasDerivedFrom(doi:trainedModelConnector, doi:trainTrainingTilesConnector)
                specializationOf(trainedModel,doi:trainedModelConnector)
                agent(def:researchDataCentre,[prov:type = 'cpm:senderAgent'])
                wasGeneratedBy(doi:trainedModelConnector,def:training,-)
                used(prostateTrain,configTrain,-)
                activity(def:training,-,-,[prov:type = 'cpm:mainActivity', cpm:metabundle = 'meta:trainMeta', repr:hasPart = 'def:prostateTrain'])
                used(def:training,doi:trainTrainingTilesConnector,-)
                activity(prostateTrain,-,-)
                wasDerivedFrom(configTrain, configFile)
                wasAttributedTo(doi:trainTrainingTilesConnector, def:researchDataCentre)
                entity(trainingTilesDataset,[prov:type = 'repr:Dataset'])
                used(prostateTrain,trainingTilesDataset,-)
                wasDerivedFrom(trainedModel, trainingTilesDataset)
                entity(trainedModel)
                specializationOf(trainingTilesDataset,doi:trainTrainingTilesConnector)
                wasDerivedFrom(trainedModel, configTrain)
                entity(doi:trainTrainingTilesConnector,[prov:type = 'cpm:backwardConnector', cpm:receiverBundleId = 'bndl:preprocTrain'])
                entity(configFile,[path = "mlflow-artifacts:/4/6bc00f9abbd0465d865f0c1e1fa7196a/artifacts/conf/config_resolved.yaml" %% xsd:string])
                entity(configTrain)
                wasGeneratedBy(trainedModel,prostateTrain,-)
                endBundle
                endDocument
                """;
    }

    private String getFirstDoc() {
        return """
                document
                default <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix def <https://example.org/>
                prefix repr <https://w3id.org/reproduceme#>
                prefix pre_0 <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix doi <https://doi.org/10.58092/>
                prefix cpm <https://commonprovenancemodel.org/ns/cpm/>
                prefix bndl <http://_PLACEHOLER_:8000/api/v1/organizations/ORG/graphs/>
                prefix meta <http://_PLACEHOLER_:8000/api/v1/graphs/meta/>
                bundle bndl:train
                default <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix def <https://example.org/>
                prefix repr <https://w3id.org/reproduceme#>
                prefix pre_0 <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                prefix doi <https://doi.org/10.58092/>
                prefix cpm <https://commonprovenancemodel.org/ns/cpm/>
                prefix bndl <http://_PLACEHOLER_:8000/api/v1/organizations/ORG/graphs/>
                prefix meta <http://_PLACEHOLER_:8000/api/v1/graphs/meta/>
                                
                entity(doi:trainedModelConnector,[prov:type = 'cpm:forwardConnector', cpm:receiverBundleId = 'bndl:eval'])
                specializationOf(trainedModel,doi:trainedModelConnector)
                entity(configFile,[path = "mlflow-artifacts:/4/6bc00f9abbd0465d865f0c1e1fa7196a/artifacts/conf/config_resolved.yaml" %% xsd:string])
                wasGeneratedBy(doi:trainedModelConnector,def:training,-)
                used(prostateTrain,configTrain,-)
                activity(def:training,-,-,[prov:type = 'cpm:mainActivity', cpm:metabundle = 'meta:trainMeta', repr:hasPart = 'def:prostateTrain'])
                activity(prostateTrain,-,-)
                wasDerivedFrom(configTrain, configFile)
                entity(trainingTilesDataset,[prov:type = 'repr:Dataset'])
                used(prostateTrain,trainingTilesDataset,-)
                wasDerivedFrom(trainedModel, trainingTilesDataset)
                entity(trainedModel)
                specializationOf(trainingTilesDataset,doi:trainTrainingTilesConnector)
                wasDerivedFrom(trainedModel, configTrain)
                entity(configTrain)
                wasGeneratedBy(trainedModel,prostateTrain,-)
                endBundle
                endDocument
                """;
    }

    private Document getDsDoc() {

        String docString = """
                document
                  default <https://gitlab.ics.muni.cz/422328/pid-test/-/blob/master/>
                  prefix bndl <http://_PLACEHOLER_:8000/api/v1/organizations/ORG/graphs/>
                  prefix mmci <https://gitlab.ics.muni.cz/422328/pid-mmci/-/blob/master/output/provn_pid/provn/>
                  prefix doi <https://doi.org/10.58092/>
                  prefix cpm <https://www.commonprovenancemodel.org/cpm-namespace-v1-0/>
                  prefix repr <https://w3id.org/reproduceme#>
                                
                  bundle bndl:train
                    entity(trainingTilesDataset, [prov:type='repr:Dataset'])
                    specializationOf(trainingTilesDataset, doi:trainTrainingTilesConnector)
                    entity(configFile, [path="mlflow-artifacts:/4/6bc00f9abbd0465d865f0c1e1fa7196a/artifacts/conf/config_resolved.yaml"])
                    wasDerivedFrom(configTrain, configFile, -, -, -)
                    entity(configTrain, [])
                    entity(trainedModel)
                    activity(prostateTrain)
                    wasGeneratedBy(trainedModel, prostateTrain, -)
                    wasDerivedFrom(trainedModel, trainingTilesDataset, -, -, -)
                    wasDerivedFrom(trainedModel, configTrain, -, -, -)
                    used(prostateTrain, trainingTilesDataset, -)
                    used(prostateTrain, configTrain, -)
                    specializationOf(trainedModel, doi:trainedModelConnector)
                  endBundle
                endDocument""";

        InputStream is = new ByteArrayInputStream(docString.getBytes());
        try {
            return intF.readDocument(is, Formats.ProvFormat.PROVN);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createBbBindings() {

        String bbString = """
                {
                  "var":
                  {
                    "bndl": [{"@id": "bndl:train"}],
                    "send_agent_id": [{"@id": "def:researchDataCentre"}],
                    "main_activity_id": [{"@id": "def:training"}],
                    "main_activity_meta": [{"@id": "meta:trainMeta"}],
                    "has_part": [{"@id": "def:prostateTrain"}],
                    "back_conn_id": [{"@id": "doi:trainTrainingTilesConnector"}],
                    "send_id": [{"@id": "bndl:preprocTrain"}],
                    "forward_conn_id": [{"@id": "doi:trainedModelConnector"}],
                    "rec_id": [{"@id": "bndl:eval"}]
                  },
                  "context": {
                    "def" : "https://example.org/",
                    "bndl": "http://_PLACEHOLER_:8000/api/v1/organizations/ORG/graphs/",
                    "meta" : "http://_PLACEHOLER_:8000/api/v1/graphs/meta/",
                    "doi" : "https://doi.org/10.58092/"
                  }
                }""";

        try {
            Files.writeString(backboneBindingsPath, bbString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
