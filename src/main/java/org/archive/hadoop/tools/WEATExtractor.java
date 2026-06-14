package org.archive.hadoop.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.extract.ExtractingResourceFactoryMapper;
import org.archive.extract.ExtractingResourceProducer;
import org.archive.extract.ExtractorOutput;
import org.archive.extract.ProducerUtils;
import org.archive.extract.ResourceFactoryMapper;
import org.archive.extract.WATExtractorOutput;
import org.archive.extract.WETExtractorOutput;
import org.archive.format.json.JSONUtils;
import org.archive.resource.Resource;
import org.archive.resource.ResourceProducer;
import org.archive.resource.html.HTMLResource;
import org.archive.resource.warc.record.WARCMetaDataResource;

import com.github.openjson.JSONArray;
import com.github.openjson.JSONException;
import com.github.openjson.JSONObject;

/**
 * Standalone WEAT extractor. Local equivalent of WEATGenerator's inner mapper.
 *
 * For each input WARC/ARC file produces:
 *   <output-dir>/wat/<basename>.wat.gz
 *   <output-dir>/wet/<basename>.wet.gz
 *
 * Usage:
 *   WEATExtractor [--skip-existing] <input-dir|input-list-file> <output-dir>
 */
public class WEATExtractor {

    public final static String TOOL_NAME = "WEATExtractor";
    public final static String TOOL_DESCRIPTION = "Standalone WEAT extractor: generates WAT and WET files from local WARC/ARC files.";

    private final File watDir;
    private final File wetDir;
    private final boolean skipExisting;

    public WEATExtractor(File outputDir, boolean skipExisting) {
        this.watDir = new File(outputDir, "wat");
        this.wetDir = new File(outputDir, "wet");
        this.skipExisting = skipExisting;
    }

    // -------------------------------------------------------------------------
    // Directory input
    // -------------------------------------------------------------------------

    public void processDirectory(File inputDir) throws IOException {
        if (!inputDir.isDirectory()) {
            throw new IOException("Not a directory: " + inputDir);
        }
        List files = collectWarcFiles(inputDir);
        if (files.isEmpty()) {
            System.err.println("No WARC/ARC files found in: " + inputDir);
            return;
        }
        System.out.println("Found " + files.size() + " file(s) to process.");
        for (int i = 0; i < files.size(); i++) {
            File f = (File) files.get(i);
            System.out.println("Processing: " + f.getName());
            try {
                processEntry(f.getAbsolutePath(), f.getName());
            } catch (Exception e) {
                System.err.println("Error processing " + f.getName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private List collectWarcFiles(File dir) {
        List result = new ArrayList();
        File[] files = dir.listFiles();
        if (files == null) {
            return result;
        }
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isFile() && isWarcFile(f.getName())) {
                result.add(f);
            }
        }
        Collections.sort(result, new Comparator() {
            public int compare(Object a, Object b) {
                return ((File) a).getName().compareTo(((File) b).getName());
            }
        });
        return result;
    }

    private boolean isWarcFile(String name) {
        if (name.endsWith(".warc.gz")) {
            return true;
        } else if (name.endsWith(".warc")) {
            return true;
        } else if (name.endsWith(".arc.gz")) {
            return true;
        } else if (name.endsWith(".arc")) {
            return true;
        } else {
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // List-file input (same format as MapReduce text input)
    // -------------------------------------------------------------------------

    public void processListFile(File listFile) throws IOException {
        List lines = readLines(listFile);
        System.out.println("Found " + lines.size() + " line(s) to process.");
        for (int i = 0; i < lines.size(); i++) {
            String line = ((String) lines.get(i)).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            String name;
            String url = line;
            int idx = line.indexOf(' ');
            if (idx == -1) {
                URL tmpUrl = new URL(line);
                name = tmpUrl.getPath();
                if (name.contains("/")) {
                    name = name.substring(name.lastIndexOf('/') + 1);
                }
            } else {
                name = line.substring(0, idx).trim();
                url  = line.substring(idx + 1).trim();
                if (name.isEmpty() || url.isEmpty()) {
                    System.err.println("Bad input line (skipping): " + line);
                    continue;
                }
            }

            System.out.println("Processing: " + name);
            try {
                processEntry(url, name);
            } catch (Exception e) {
                System.err.println("Error processing " + name + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private List readLines(File file) throws IOException {
        List lines = new ArrayList();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    // ignore close error
                }
            }
        }
        return lines;
    }

    // -------------------------------------------------------------------------
    // Core extraction — mirrors WEATGeneratorMapper.map() exactly,
    // replacing HDFS FileSystem / Path with local java.io.File.
    // -------------------------------------------------------------------------

    private void processEntry(String path, String inputBasename) throws IOException {
        String watBasename;
        String wetBasename;
        if (path.endsWith(".gz")) {
            String stem = inputBasename.substring(0, inputBasename.length() - 3);
            watBasename = stem + ".wat.gz";
            wetBasename = stem + ".wet.gz";
        } else {
            watBasename = inputBasename + ".wat.gz";
            wetBasename = inputBasename + ".wet.gz";
        }

        File watFile = new File(watDir, watBasename);
        File wetFile = new File(wetDir, wetBasename);

        if (skipExisting && watFile.exists() && wetFile.exists()) {
            System.out.println("  Skipping (wat+wet already exist): " + inputBasename);
            return;
        }

        Logger.getLogger("org.archive.format.gzip.GZIPMemberSeries").setLevel(Level.WARNING);
        Logger.getLogger("org.archive.extract.ExtractingResourceProducer").setLevel(Level.WARNING);

        ResourceProducer producer = ProducerUtils.getProducer(path);
        if (producer == null) {
            System.err.println("  Could not open producer for: " + path);
            return;
        }

        ResourceFactoryMapper mapper = new ExtractingResourceFactoryMapper();
        ExtractingResourceProducer exProducer = new ExtractingResourceProducer(producer, mapper);

        FileOutputStream watFos = null;
        FileOutputStream wetFos = null;
        try {
            watFos = new FileOutputStream(watFile);
            wetFos = new FileOutputStream(wetFile);

            ExtractorOutput watOut = new WATExtractorOutput(watFos);
            ExtractorOutput wetOut = new WETExtractorOutput(wetFos, wetBasename);

            int count = 0;
            Resource lr = null;
            while (count < Integer.MAX_VALUE) {
                Resource r = null;
                try {
                    r = exProducer.getNext();
                } catch (Exception e) {
                    System.err.println("  Error reading record: " + e.getMessage());
                    e.printStackTrace();
                    continue;
                }
                if (r == null) {
                    break;
                }
                count++;
                if (count % 1000 == 0) {
                    System.out.println("  " + count + " records processed.");
                }
                watOut.output(r);
                if (lr != null && isMetaConcurrentTo(r, lr)) {
                    JSONArray payloadMetadata = JSONUtils.extractArray(
                            r.getMetaData().getTopMetaData(),
                            "Envelope.Payload-Metadata.WARC-Metadata-Metadata.Metadata-Records");
                    if (payloadMetadata != null) {
                        JSONObject lheaders = JSONUtils.extractObject(
                                lr.getMetaData().getTopMetaData(),
                                "Envelope.WARC-Header-Metadata");
                        if (lheaders != null) {
                            addIdentifiedContentLanguage(lheaders, payloadMetadata);
                        }
                    }
                }
                if (lr != null) {
                    wetOut.output(lr);
                }
                lr = r;
            }
            if (lr != null) {
                wetOut.output(lr);
            }
        } finally {
            if (watFos != null) {
                try {
                    watFos.close();
                } catch (IOException e) {
                    // ignore close error
                }
            }
            if (wetFos != null) {
                try {
                    wetFos.close();
                } catch (IOException e) {
                    // ignore close error
                }
            }
        }
    }

    private static boolean isMetaConcurrentTo(Resource meta, Resource response) {
        if (meta instanceof WARCMetaDataResource && response instanceof HTMLResource) {
            JSONObject mheaders = JSONUtils.extractObject(
                    meta.getMetaData().getTopMetaData(), "Envelope.WARC-Header-Metadata");
            JSONObject headers = JSONUtils.extractObject(
                    response.getMetaData().getTopMetaData(), "Envelope.WARC-Header-Metadata");
            if (mheaders == null || headers == null) {
                return false;
            }
            String mu = mheaders.optString("WARC-Target-URI");
            String u  = headers.optString("WARC-Target-URI");
            if (u.equals(mu) && !u.isEmpty()) {
                String muid = mheaders.optString("WARC-Concurrent-To");
                String uid  = headers.optString("WARC-Record-ID");
                if (!uid.isEmpty() && uid.equals(muid)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void addIdentifiedContentLanguage(JSONObject headers, JSONArray payloadMetadata) {
        for (int i = 0; i < payloadMetadata.length(); i++) {
            JSONObject entry = payloadMetadata.getJSONObject(i);
            if (!"languages-cld2".equals(entry.opt("Name"))) {
                continue;
            }
            String val = entry.optString("Value");
            if (val.isEmpty()) {
                continue;
            }
            try {
                JSONObject jsonVal = new JSONObject(val);
                if (!jsonVal.has("languages")) {
                    continue;
                }
                JSONArray langs = jsonVal.getJSONArray("languages");
                String identifiedLanguages;
                if (langs.length() == 1) {
                    identifiedLanguages = langs.getJSONObject(0).getString("code-iso-639-3");
                } else {
                    StringBuffer sb = new StringBuffer();
                    String[] dedup = { "", "" };
                    for (int j = 0; j < langs.length(); j++) {
                        String lang = langs.getJSONObject(j).optString("code-iso-639-3");
                        if (lang == null || "null".equals(lang)) {
                            continue;
                        }
                        if (j > 0 && dedup[0].equals(lang)) {
                            continue;
                        }
                        if (j > 1 && dedup[1].equals(lang)) {
                            continue;
                        }
                        if (sb.length() > 0) {
                            sb.append(',');
                        }
                        sb.append(lang);
                        if (j < 2) {
                            dedup[j] = lang;
                        }
                    }
                    identifiedLanguages = sb.toString();
                }
                if (!identifiedLanguages.isEmpty()) {
                    headers.put("WARC-Identified-Content-Language", identifiedLanguages);
                }
            } catch (JSONException e) {
                System.err.println("  Failed to parse CLD2 result: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        boolean skipExisting = false;
        int i = 0;
        while (i < args.length && args[i].startsWith("--")) {
            if (args[i].equals("--skip-existing")) {
                skipExisting = true;
            }
            i++;
        }
        if (args.length - i != 2) {
            printUsage();
            System.exit(1);
        }

        File input     = new File(args[i]);
        File outputDir = new File(args[i + 1]);

        if (!input.exists()) {
            System.err.println("Input not found: " + input);
            System.exit(1);
        }
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                System.err.println("Cannot create output directory: " + outputDir);
                System.exit(1);
            }
        }

        File watDir = new File(outputDir, "wat");
        File wetDir = new File(outputDir, "wet");
        if (!watDir.exists() && !watDir.mkdirs()) {
            System.err.println("Cannot create wat directory: " + watDir);
            System.exit(1);
        }
        if (!wetDir.exists() && !wetDir.mkdirs()) {
            System.err.println("Cannot create wet directory: " + wetDir);
            System.exit(1);
        }

        WEATExtractor extractor = new WEATExtractor(outputDir, skipExisting);

        if (input.isDirectory()) {
            extractor.processDirectory(input);
        } else {
            extractor.processListFile(input);
        }

        System.out.println("Done.");
    }

    private static void printUsage() {
        System.err.println("Usage: WEATExtractor [--skip-existing] <input> <output-dir>");
        System.err.println();
        System.err.println("  <input>        a directory of WARC/ARC files, or a text file listing them.");
        System.err.println("                 Text file lines: FilePath  or  BASENAME FilePath");
        System.err.println("  <output-dir>   base directory; WAT files go to <output-dir>/wat/,");
        System.err.println("                 WET files go to <output-dir>/wet/");
        System.err.println("  --skip-existing  skip files where both .wat.gz and .wet.gz already exist");
    }
}
