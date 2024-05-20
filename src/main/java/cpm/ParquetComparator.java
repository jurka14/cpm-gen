package cpm;

import com.exasol.parquetio.data.Row;
import com.exasol.parquetio.reader.RowParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ParquetComparator {

    private ParquetComparator() {}

    /**
     * Checks the lists of parquet files against each other for disjunction.
     * @param dataLists List of lists of parquet files. Only the first two lists are compared.
     * @return List of wsi files present in both datasets.
     */
    public static List<String> getEquals(List<List<Path>> dataLists) {
        List<String> equals = new ArrayList<>();

        for (Path p : dataLists.get(0)) {
            equals.addAll(checkDatasets(p, dataLists.get(1)));
        }

        return equals;
    }

    private static List<String> checkDatasets(Path path, List<Path> pathList) {
        List<String> equals = new ArrayList<>();

        for (Path p : pathList) {
            equals.addAll(checkFiles(path, p));
        }

        return equals;
    }

    /** Compares the WSI path values in the parquet files */
    private static List<String> checkFiles(Path path1, Path path2) {

        try (
                ParquetReader<Row> reader1 = RowParquetReader
                        .builder(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path1.toString()), new Configuration())).build();
                ParquetReader<Row> reader2 = RowParquetReader
                        .builder(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path2.toString()), new Configuration())).build()
        ) {
            Row row1 = reader1.read();
            Row row2 = reader2.read();
            String wsiPath1;
            String wsiPath2;
            List<String> equals = new ArrayList<>();


            while (row1 != null) {

                wsiPath1 = (String) row1.getValue(4);

                while (row2 != null) {
                    wsiPath2 = (String) row2.getValue(4);

                    if (Objects.equals(wsiPath1, wsiPath2)) {
                        equals.add(wsiPath1);
                    }

                    row2 = reader2.read();
                }

                row1 = reader1.read();
            }

            return equals;

        } catch (final IOException exception) {
            throw new RuntimeException("Parquet file error.", exception);
        }
    }

}
