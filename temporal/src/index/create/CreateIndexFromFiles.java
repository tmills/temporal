package index.create;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class CreateIndexFromFiles {

	public static void main(String[] args) throws IOException {
		if(args.length < 2){
		  System.err.println("Two required arguments: <Data directory> <Index directory>");
		  System.exit(-1);
		}
		Directory directory = FSDirectory.open(Paths.get(args[1]));
		Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
		IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);

		File rootDir = new File(args[0]);
		for(File dir : rootDir.listFiles()) {
			for(File file : dir.listFiles()) {
				String text = Files.toString(file, Charsets.UTF_8);
				Document document = new Document();
				document.add(new TextField("content", text, Field.Store.YES));
				indexWriter.addDocument(document);
			}
		}

		indexWriter.close();
		System.out.println("done!");
	}
}
