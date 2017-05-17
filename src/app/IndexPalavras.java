package app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class IndexPalavras {
	
	private int wpc = 0;
	private Integer processados = 0;
	
	// Guarda palavras e seus respectivos ids
	private Map<String, Integer> hashPalavras = new HashMap<>();
	// Guarda livros e seus respectivos ids 
	private Map<Integer, String> hashLivros = new HashMap<>();
	// Guarda o id da palavra e o livro/linha onde se encontra
	private Map<Integer, Map<Integer, List<Integer>>> hashPalavraLivros = new ConcurrentHashMap<>();
	
	private int lastPalavraId = 0;
	private int lastLivroId = 0;
	
	private final String LIVROS_PATH = "./livros/";
	
	public IndexPalavras() throws Exception {
		
		int opc = 0;
		do {
			opc = getMenuOption();
			if (opc != 0) {
				processaMenuOption(opc);
			} else {
				break;
			}
		} while(true);
		
		System.out.println("Encerrado.");

	}
	private int getMenuOption() {
		Integer opc = 0;
		print("========================================");
		print("Digite uma opção:");
		print("1 - Buscar palavra");
		print("2 - Reindexar");
		print("0 - Sair");

		do {
			String input = Util.inputRead("Opção: ");
			try {
				opc = Integer.parseInt(input);
				if (opc > 2 || opc < 0) {
					print("Opção inválida.");
				} else {
					break;
				}
			} catch(Exception ex) {
				ex.printStackTrace();
			}
		} while(true);
	
		return opc;
	}
	
	private void doBuscarPalavras() {
		boolean exit = false;
		Scanner keyboard = new Scanner(System.in);
		do {
			System.out.println("(0 = sair) Digite uma palavra: ");
			String palavra = keyboard.nextLine();
			if (!"0".equals(palavra)) {
				palavra = palavra.toLowerCase();
				if (!hashPalavras.containsKey(palavra)){
					System.out.println("Palavra não encontrada!");
				} else {
					int idPalavra = hashPalavras.get(palavra);
					Map<Integer, List<Integer>> livrosEncontrados = hashPalavraLivros.get(idPalavra);
					for (Map.Entry<Integer, List<Integer>> livro : livrosEncontrados.entrySet()) {
					    Integer livroId = livro.getKey();
					    List<Integer> linhasEncontradas = livro.getValue();
					    String filename = this.hashLivros.get(livroId);
					    this.buscaLinhasLivro(palavra, filename, linhasEncontradas);
					}
				}
			} else {
				exit = true;
			}
		} while(!exit);
		keyboard.close();
	}
	private void doReindexar() throws Exception {
		long t1 = 0;
		long t2 = 0;
		
		File folder = new File(LIVROS_PATH);
		
		File[] listFiles;
		
		if (folder.exists() && folder.canRead()) {
			listFiles = folder.listFiles();
		} else {			
			throw new Exception("Não foi possível abrir a pasta ./livros/");
		}
		
		
		t1 = System.nanoTime();
		
		final int tamanhoTotal = listFiles.length;
		
		ThreadPoolExecutor execService = (ThreadPoolExecutor) Executors.newFixedThreadPool(16);
		for (File fp : listFiles) {
			execService.execute(() -> {
				String filename = fp.getName();
				long t4 = System.nanoTime();
				processaArquivo(filename);
				long t5 = System.nanoTime();
				System.out.println((++processados)+"/"+tamanhoTotal+" Time " + ((t5 - t4) / 1000000.0) +" WPC "+wpc+" -> " + filename);
			});
		}
		
		execService.shutdown();
		while(!execService.isTerminated()){}
		
		t2 = System.nanoTime();
		
		System.out.println("Número de palavras: " + hashPalavras.size());
		System.out.println("Número de Livros: " + hashLivros.size());	
		System.out.println("Time Indexing " + ((t2 - t1) / 1000000.0));
	}
	
	private void processaMenuOption(int option) throws Exception {
		switch(option) {
			case 1: 
				doBuscarPalavras();
				break;
			case 2:
				doReindexar();
				break;
		}
	}
		
	private void print(String str) {
		System.out.println(str);
	}
	
	private void buscaLinhasLivro(String palavra, String filename, List<Integer> linhasEncontradas) {
		try {
			FileReader fr = new FileReader(LIVROS_PATH + filename);
			BufferedReader bfr = new BufferedReader(fr);
			String line = "";
			int lineNumber = 0; // controla o número da linha
			Collections.sort(linhasEncontradas);
			int firstLine = linhasEncontradas.get(0);
			int lastLine = linhasEncontradas.get(linhasEncontradas.size()-1);
			
			while ((line = bfr.readLine()) != null) {
				line = this.normalizeLine(line); // usando a mesma normalização da indexação
				
				//A primeira condição é tende a ser mais rápida, eliminando a segunda quando não necessário. 
				if (firstLine >= lineNumber && linhasEncontradas.contains(lineNumber))	{			
					System.out.println(filename + " | linha " + lineNumber + " -> " + line.replaceAll("\\b"+palavra+"\\b", "<<"+palavra+">>"));
				}
				lineNumber++;
				
				//Evita percorrer o livro até o final, quando não necessário.
				if (lineNumber > lastLine) {
					break;
				}
			}
			bfr.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void processaArquivo(String filename) {
		try {
			FileReader fr = new FileReader(LIVROS_PATH + filename);
			BufferedReader bfr = new BufferedReader(fr);
			
			// Colocar livro no hash de livros gerando um id
			int livroId = this.getLivroId(filename);

			String line = "";

			int lineNumber = 0; // controla o número da linha
			while ((line = bfr.readLine()) != null) {
				
				line = this.normalizeLine(line);
				String strs[] = line.split(" ");
				
				for (String str : strs) {
					if (str.length() > 0 && str.length() < 30) {	
						
						// Coloca palavra no hash (se não existe) e retorna um id
						int palavraId = this.getPalavraId(str);
						
						if (this.hashPalavraLivros.containsKey(palavraId)) { // Palavra já existe
							Map<Integer, List<Integer>> livroLinhas = this.hashPalavraLivros.get(palavraId);
							if (livroLinhas.containsKey(livroId)) { // Livro já existe na palavra
								List<Integer> linhas = livroLinhas.get(livroId);
								if (!linhas.contains(lineNumber)){ // Caso exista a mesma palavra no mesmo livro e na mesma linha 
									linhas.add(lineNumber); // Adiciona linha
								}
							} else {
								List<Integer> linhas = new ArrayList<>();
								linhas.add(lineNumber);
								livroLinhas.put(livroId, linhas);
							}
						} else {
							//Porque ConcurrentHashMap aqui? Pode ser HashMap?
							//Map<Integer, List<Integer>> livroLinhas = new ConcurrentHashMap<Integer, List<Integer>>();
							Map<Integer, List<Integer>> livroLinhas = new HashMap<Integer, List<Integer>>();
							List<Integer> linhas = new ArrayList<>();
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
			fr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
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
