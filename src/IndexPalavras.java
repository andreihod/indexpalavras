import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class IndexPalavras {
	
	public int wpc = 0;
	public int processados = 0;
	
	// Guarda palavras e seus respectivos ids
	private HashMap<String, Integer> hashPalavras = new HashMap<>();
	// Guarda livros e seus respectivos ids 
	private HashMap<Integer, String> hashLivros = new HashMap<>();
	// Guarda o id da palavra e o livro/linha onde se encontra
	private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, ArrayList<Integer>>> hashPalavraLivros = new ConcurrentHashMap<>();
	
	private int lastPalavraId = 0;
	private int lastLivroId = 0;
	
	public IndexPalavras() {
		
		long t1 = 0;
		long t2 = 0;
		
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
		
		System.out.println("Número de palavras: " + hashPalavras.size());
		System.out.println("Número de Livros: " + hashLivros.size());	

		System.out.println("Time Indexing " + ((t2 - t1) / 1000000.0));
		
		Scanner keyboard = new Scanner(System.in);
		
		boolean exit = false;
		while(!exit){
			System.out.println("(0 = sair) Digite uma palavra: ");
			String palavra = keyboard.nextLine();
			if (!palavra.equals("0")) {
				palavra = palavra.toLowerCase();
				if (!hashPalavras.containsKey(palavra)){
					System.out.println("Palavra não encontrada!");
				} else {
					int idPalavra = hashPalavras.get(palavra);
					ConcurrentHashMap<Integer, ArrayList<Integer>> livrosEncontrados = hashPalavraLivros.get(idPalavra);
					for (Map.Entry<Integer, ArrayList<Integer>> livro : livrosEncontrados.entrySet()) {
					    Integer livroId = livro.getKey();
					    ArrayList<Integer> linhasEncontradas = livro.getValue();
					    String filename = this.hashLivros.get(livroId);
					    this.buscaLinhasLivro(palavra, filename, linhasEncontradas);
					}
				}
			} else {
				exit = true;
			}
		}
		
		keyboard.close();		
		System.out.println("Encerrando");

	}

	private void buscaLinhasLivro(String palavra, String filename, ArrayList<Integer> linhasEncontradas) {
		try {
			FileReader fr = new FileReader(filename);
			BufferedReader bfr = new BufferedReader(fr);
			String line = "";
			int lineNumber = 0; // controla o número da linha
			while ((line = bfr.readLine()) != null) {
				line = this.normalizeLine(line); // usando a mesma normalização da indexação
				if (linhasEncontradas.contains(lineNumber))				
					System.out.println(filename + " | linha " + lineNumber + " -> " + line.replaceAll("\\b"+palavra+"\\b", "<<"+palavra+">>"));
				lineNumber++;
			}
			bfr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processaArquivo(String filename) {
		try {
			FileReader fr = new FileReader(filename);
			BufferedReader bfr = new BufferedReader(fr);
			
			// Colocar livro no hash de livros gerando um id
			int livroId = this.getLivroId(filename);

			String line = "";

			int lineNumber = 0; // controla o número da linha
			while ((line = bfr.readLine()) != null) {
				
				line = this.normalizeLine(line);
				String str[] = line.split(" ");
				
				for (int i = 0; i < str.length; i++) {
					if (str[i].length() > 0 && str[i].length() < 30) {	
						
						// Coloca palavra no hash (se não existe) e retorna um id
						int palavraId = this.getPalavraId(str[i]);
						
						if (this.hashPalavraLivros.containsKey(palavraId)) { // Palavra já existe
							ConcurrentHashMap<Integer, ArrayList<Integer>> livroLinhas = this.hashPalavraLivros.get(palavraId);
							if (livroLinhas.containsKey(livroId)) { // Livro já existe na palavra
								ArrayList<Integer> linhas = livroLinhas.get(livroId);
								if (!linhas.contains(lineNumber)){ // Caso exista a mesma palavra no mesmo livro e na mesma linha 
									linhas.add(lineNumber); // Adiciona linha
								}
							} else {
								ArrayList<Integer> linhas = new ArrayList<Integer>();
								linhas.add(lineNumber);
								livroLinhas.put(livroId, linhas);
							}
						} else {
							ConcurrentHashMap<Integer, ArrayList<Integer>> livroLinhas = new ConcurrentHashMap<Integer, ArrayList<Integer>>();
							ArrayList<Integer> linhas = new ArrayList<Integer>();
							linhas.add(lineNumber);
							livroLinhas.put(livroId, linhas);
							this.hashPalavraLivros.put(palavraId, livroLinhas);
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
	
	private String normalizeLine(String line) {
		line = Normalizer.normalize(line, Normalizer.Form.NFD);
		line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
		line = line.replaceAll("[^a-zA-Z ]", " ");
		line = line.toLowerCase();
		return line;
	}

	private synchronized int getPalavraId(String palavra) {
		int id = 0;
		if (this.hashPalavras.containsKey(palavra)) {
			id = this.hashPalavras.get(palavra);
		} else {
			id = this.getNextPalavraId();
			this.hashPalavras.put(palavra, id);
		}
		return id;
	}
	
	private synchronized int getLivroId(String livro) {
		// Não é necessário checar se já exste, nunca vai repetir o mesmo livro
		int id = this.getNextLivroId();
		this.hashLivros.put(id, livro);
		return id;
	}
	
	private synchronized int getNextPalavraId(){
		return this.lastPalavraId++;
	}
	
	private synchronized int getNextLivroId(){
		return this.lastLivroId++;
	}
	
}
