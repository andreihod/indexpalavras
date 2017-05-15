import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Normalizer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class IndexPalavras {
	
	public int wpc = 0;
	public int processados = 0;
	
	// Guarda palavras e seus respectivos ids
	private ConcurrentHashMap<String, Integer> hashPalavras = new ConcurrentHashMap<>();
	// Guarda livros e seus respectivos ids 
	private ConcurrentHashMap<String, Integer> hashLivros = new ConcurrentHashMap<>();
	// Guarda o id da palavra e o livro/linha onde se encontra
	private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Integer>> hashPalavraLivros = new ConcurrentHashMap<>();
	
	private int lastPalavraId = 0;
	private int lastLivroId = 0;
	
	public IndexPalavras() {
		
		long t1 = 0;
		long t2 = 0;
		long t3 = 0;
		
		File folder = new File("./livros/");
		File[] listOfFiles = folder.listFiles();
		
		final ConcurrentLinkedQueue<File> listfiles = new ConcurrentLinkedQueue<>();
		
		t1 = System.nanoTime();
		
		for (int j = 0; j < listOfFiles.length; j++) {
			listfiles.add(listOfFiles[j]);
		}
		
		final int tamanhototal = listfiles.size();
		
		int ntread = 16;
		Thread threads[] = new Thread[ntread];
		
		for(int i = 0; i < ntread;i++){
			threads[i] = new Thread(new Runnable() {
				public void run() {
					long t4 = 0;
					long t5 = 0;
					
					while(listfiles.size()>0) {
						String filename = listfiles.poll().getPath();
						t4 = System.nanoTime();
						
						processaArquivo(filename);
						
						t5 = System.nanoTime();
						System.out.println(""+(processados+1)+"/"+tamanhototal+" Time " + ((t5 - t4) / 1000000.0) +" WPC "+wpc+" ->" + filename);
						processados++;
					}
				}
			},"T"+i);
		}
		
		for(int i = 0; i < ntread;i++){
			threads[i].start();
		}
		
		for(int i = 0; i < ntread;i++){
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		t2 = System.nanoTime();

		System.out.println("Time Processing " + ((t2 - t1) / 1000000.0));
		//System.out.println("Time Sort " + ((t3 - t2) / 1000000.0));
	}

	private void processaArquivo(String filename) {
		try {
			FileReader fr = new FileReader(filename);
			BufferedReader bfr = new BufferedReader(fr);

			String line = "";

			int lineNumber = 0;
			while ((line = bfr.readLine()) != null) {
				line = Normalizer.normalize(line, Normalizer.Form.NFD);
				line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
				line = line.replaceAll("[^a-zA-Z ]", " ");
				line = line.toLowerCase();
				String str[] = line.split(" ");
				for (int i = 0; i < str.length; i++) {
					if (str[i].length() > 0 && str[i].length() < 30) {						
						int palavraId = this.getPalavraId(str[i]);
						int livroId = this.getLivroId(filename);
						if (hashPalavraLivros.containsKey(palavraId)) {
							ConcurrentHashMap<Integer, Integer> livroLinhas = hashPalavraLivros.get(palavraId);
							if (livroLinhas.containsKey(livroId)) {
								
							} else {
								
							}
						} else {
							ConcurrentHashMap<Integer, Integer> livroLinhas = new ConcurrentHashMap<Integer, Integer>();
							livroLinhas.put(livroId, lineNumber);
							//TO-DO: Precisa poder adicionar várias linhas para o mesmo livro, usar ArrayList?
							hashPalavraLivros.put(palavraId, livroLinhas);
						}
						wpc++;
					}
				}
				lineNumber++;
			}
			bfr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private int getPalavraId(String palavra) {
		int id = 0;
		if (this.hashPalavras.containsKey(palavra)) {
			id = this.hashPalavras.get(palavra);
		} else {
			id = this.getNextPalavraId();
			this.hashPalavras.put(palavra, id);
		}
		return id;
	}
	
	private int getLivroId(String livro) {
		int id = 0;
		if (this.hashLivros.containsKey(livro)) {
			id = this.hashLivros.get(livro);
		} else {
			id = this.getNextPalavraId();
			this.hashLivros.put(livro, id);
		}
		return id;
	}
	
	private synchronized int getNextPalavraId(){
		return this.lastPalavraId++;
	}
	
	private synchronized int getNextLivroId(){
		return this.lastLivroId++;
	}
	
}
