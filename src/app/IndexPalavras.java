package app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class IndexPalavras {
	private final String INDEX_FILE = "INDEX.csv";
	private final String LIVROS_FILE = "LIVROS.csv";
	private final String PALAVRAS_FILE = "PALAVRAS.csv";
		
	private int wpc = 0;
	private Integer processados = 0;
	
	// Guarda palavras e seus respectivos ids
	private Map<String, Integer> hashPalavras = new HashMap<>();
	// Guarda livros e seus respectivos ids 
	private Map<Integer, String> hashLivros = new HashMap<>();
	// Guarda o id da palavra e o livro/linha onde se encontra
//	private Map<Integer, Map<Integer, List<Integer>>> hashPalavraLivros = new ConcurrentHashMap<>();
	
	private Map<Integer, Palavra> indexPrincipal = new ConcurrentHashMap<>();
	
	
	private int lastPalavraId = 0;
	private int lastLivroId = 0;
	
	private final String LIVROS_PATH = "./livros/";
	
	public IndexPalavras() throws Exception {
		
		try {
			doCarregarIndex();
		} catch(Exception ex) {
			print("[-] " + ex.getMessage());
			doReindexar();
		}
		
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
	
//	private void doBuscarPalavras() {
//		boolean exit = false;
//		Scanner keyboard = new Scanner(System.in);
//		do {
//			System.out.println("(0 = sair) Digite uma palavra: ");
//			String palavra = keyboard.nextLine();
//			if (!"0".equals(palavra)) {
//				palavra = palavra.toLowerCase();
//				if (!hashPalavras.containsKey(palavra)){
//					System.out.println("Palavra não encontrada!");
//				} else {
//					int idPalavra = hashPalavras.get(palavra);
//					Map<Integer, List<Integer>> livrosEncontrados = hashPalavraLivros.get(idPalavra);
//					for (Map.Entry<Integer, List<Integer>> livro : livrosEncontrados.entrySet()) {
//					    Integer livroId = livro.getKey();
//					    List<Integer> linhasEncontradas = livro.getValue();
//					    String filename = this.hashLivros.get(livroId);
//					    this.buscaLinhasLivro(palavra, filename, linhasEncontradas);
//					}
//				}
//			} else {
//				exit = true;
//			}
//		} while(!exit);
//		keyboard.close();
//	}
	
	
	private void doCarregarIndex() throws Exception {
		File fpLivros = new File(LIVROS_FILE);
		File fpPalavras = new File(PALAVRAS_FILE);
		File fpIndex = new File(INDEX_FILE);
		
		if (!(fpLivros.exists() && fpLivros.canRead() 
				&& fpPalavras.exists() && fpPalavras.canRead()
				&& fpIndex.exists() && fpIndex.canRead())) {
		
			throw new Exception("Não foi encontrado os arquivos necessários para a carga do index.");			
		}
		
		long t1 = System.nanoTime();
		
		print("== Iniciando a carregamento do index em memória. ==");
		print("Lendo o arquivo " + LIVROS_FILE);
		FileReader fReader = new FileReader(fpLivros);
		BufferedReader bfReader = new BufferedReader(fReader);
		String linha = "";
		this.hashLivros.clear();
		
		
		int contador = 0;
		while ((linha = bfReader.readLine()) != null) {
			String row[] = linha.split(";");
			hashLivros.put(Integer.parseInt(row[0]), row[1]);
			contador++;
		}
		
		bfReader.close();
		fReader.close();
		print("Total de " + contador + " livros carregados.");
		
		contador = 0;
		
		print("Lendo o arquivo " + PALAVRAS_FILE);
		
		fReader = new FileReader(fpPalavras);
		bfReader = new BufferedReader(fReader);
		hashPalavras.clear();
		while ((linha = bfReader.readLine()) != null) {
			String row[] = linha.split(";");
			hashPalavras.put(row[1], Integer.parseInt(row[0]));
			contador++;
		}
		
		bfReader.close();
		fReader.close();
		print("Total de " + contador + " palavras carregadas.");
		
		print("Lendo o arquivo " + INDEX_FILE);
		
		fReader = new FileReader(fpIndex);
		bfReader = new BufferedReader(fReader);
		
		indexPrincipal = new HashMap<>(); //Apenas para acelerar o processo!

		contador = 0;
		while ((linha = bfReader.readLine()) != null) {
			String row[] = linha.split(";");
			int palavraId = Integer.parseInt(row[0]);
			int livroId = Integer.parseInt(row[1]);
			String linhas[] = row[2].split(",");
			
			Palavra palavra = indexPrincipal.get(palavraId);
			if (palavra == null) {
				palavra = new Palavra();
				palavra.setId(palavraId);
			}
			
			Map<Integer, Livro> livros = palavra.getLivros();
			Livro livro = new Livro();
			livro.setId(livroId);
			
			for (String strLinha : linhas) {
				int linhaNumber = Integer.parseInt(strLinha);
				livro.getLinhas().add(linhaNumber);	
			}
			livros.put(livroId, livro);
			palavra.setLivros(livros);
			
			indexPrincipal.put(palavraId, palavra);
			contador++;
		}
		
		bfReader.close();
		fReader.close();
		print("Total de " + contador + " palavras indexadas.");
		
		indexPrincipal = new ConcurrentHashMap<>(indexPrincipal);
		
		long t2 = System.nanoTime();
		
		print("Conferindo Index: ");
		print("Total de Livros carregados (dicionario): " + hashLivros.size());
		print("Total de Palavras carregados (dicionario): " + hashPalavras.size());
		print("Total de palavras Indexadas.: " + indexPrincipal.size());
		print("Tempo total de carregamento:  " + ((t2 - t1) / 1000000.0));
		
		
	}
	
	private void doReindexar() throws Exception {
		long t1 = 0;
		long t2 = 0;
		this.lastLivroId = 0;
		this.lastPalavraId = 0;
		this.hashPalavras.clear();
		this.hashLivros.clear();
		this.indexPrincipal.clear();
		
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
				System.out.println((++processados)+"/"+tamanhoTotal+" Time " + ((t5 - t4) / 1000000.0) +" WPC "+ ++wpc +" -> " + filename);
			});
		}
		
		execService.shutdown();
		while(!execService.isTerminated()){}
		
		t2 = System.nanoTime();
		
		print("Número de palavras: " + hashPalavras.size());
		print("Número de Livros: " + hashLivros.size());	
		print("Tempo total Indexando:  " + ((t2 - t1) / 1000000.0));
		
		ordenarIndex();
		salvarIndex();
	}
	
	private void ordenarIndex() {
		print("Sorting... ");
		long t1 = System.nanoTime();
		Map<Integer, Palavra> treeMap = new TreeMap<>(indexPrincipal);
		indexPrincipal = new ConcurrentHashMap<Integer, Palavra>(treeMap);
		treeMap = null;
		indexPrincipal.forEach((key, palavra) -> {
			Map<Integer, Livro> livros = new TreeMap<>(palavra.getLivros());
			livros.forEach((key2, obj) -> {
				obj.setLinhas(new HashSet<>(new TreeSet<>(obj.getLinhas())));
			});
			palavra.setLivros(new ConcurrentHashMap<>(livros));
		});
		
		//Não terá sorting do hash de palavras, pois o ID é o valor. Um HashMap não garante a ordenação de valores
		// A ordenação será feita no metodo salvarIndex()
		hashLivros = new HashMap<>(new TreeMap<>(hashLivros));

		long t2 = System.nanoTime();		
		
		print("Time Sorting: " + ((t2 - t1) / 1000000.0));
	}
	
	private void salvarIndex() throws IOException {
		print("Salvando Livros no arquivo " + LIVROS_FILE + " ...");

		long t1 = System.nanoTime();
		File fp = new File(LIVROS_FILE);
		FileWriter fw = new FileWriter(fp);
		for (Entry<Integer, String> livro : hashLivros.entrySet()) {
			fw.write(livro.getKey() + ";" + livro.getValue() + "\n");			
		}
		fw.close();
		
		print("Salvando Palavras no arquivo " + PALAVRAS_FILE + " ...");
		fp = new File(PALAVRAS_FILE);
		fw = new FileWriter(fp);
		
		Map<Integer, String> listTemp = new TreeMap<>();
		hashPalavras.forEach((key, obj) -> {
			listTemp.put(obj, key);
		});
		
		for (Entry<Integer, String> entry : listTemp.entrySet()) {
			fw.write(entry.getKey() + ";" + entry.getValue() + "\n");						
		}		
		fw.close();
		
		print("Salvando INDEX no arquivo " + INDEX_FILE + " ...");
		fp = new File(INDEX_FILE);
		fw = new FileWriter(fp);
		for (Entry<Integer, Palavra> entryPalavra : indexPrincipal.entrySet()) {
			Palavra palavra = entryPalavra.getValue();
			String linha = "";
			for (Livro livro : entryPalavra.getValue().getLivros().values()) {
				linha = palavra.getId() + ";" + livro.getId() + ";";
				for (Integer x : livro.getLinhas()) {
					linha += x + ",";
				}
				linha = linha.substring(0, linha.length() -1);
				fw.write(linha + "\n");
			}
		}
		fw.close();
		
		long t2 = System.nanoTime();		
		print("INDEX SALVO!");
		print("Tempo total para salvar os dados: " + ((t2 - t1) / 1000000.0));
		
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
		
	private void doBuscarPalavras() {
		String palavra = Util.inputRead("Digite a palavra: ");
		palavra = palavra.toLowerCase();
		if (hashPalavras.containsKey(palavra)){
			int palavraId = hashPalavras.get(palavra);
			Palavra p = indexPrincipal.get(palavraId);
			Map<Integer, Livro> livros = p.getLivros();		
			this.percorreLivros(palavra, livros);
		} else {
			print("Palavra não encontrada! =(");
		}			
	}
	
	private void percorreLivros(String palavra, Map<Integer, Livro> livros) {
		for (Entry<Integer, Livro> entryLivro : livros.entrySet()) {
			Livro livro = entryLivro.getValue();
			Set<Integer> linhas = livro.getLinhas();
			this.printLinhasLivro(palavra, this.hashLivros.get(livro.getId()), Util.asSortedList(linhas));
		}
	}

	private void print(String str) {
		System.out.println(str);
	}
	
	private void printLinhasLivro(String palavra, String filename, List<Integer> linhasEncontradas) {
		try {
			FileReader fr = new FileReader(LIVROS_PATH + filename);
			BufferedReader bfr = new BufferedReader(fr);
			String line = "";
			int lineNumber = 0; // controla o número da linha
			int firstLine = linhasEncontradas.get(0);
			int lastLine = linhasEncontradas.get(linhasEncontradas.size()-1);
			while ((line = bfr.readLine()) != null) {
				line = this.normalizeLine(line); // usando a mesma normalização da indexação
				
				//A primeira condição é tende a ser mais rápida, eliminando a segunda quando não necessário. 
				if (lineNumber >= firstLine && linhasEncontradas.contains(lineNumber))	{			
					System.out.println(String.format("%-20s | %5d -> %s", filename, lineNumber, line.replaceAll("\\b"+palavra+"\\b", "<<"+palavra+">>")));
				}
				lineNumber++;
				
				//Evita percorrer o livro até o final, quando não necessário.
				if (lastLine <= lineNumber - 1) {
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
						Palavra palavra = indexPrincipal.get(palavraId);
						
						if (palavra == null) {
							palavra = new Palavra();
							palavra.setId(palavraId);
							Livro livro = new Livro();
							livro.setId(livroId);
							livro.getLinhas().add(lineNumber);
							palavra.getLivros().put(livroId, livro);
							indexPrincipal.put(palavraId, palavra);
						} else {
							Livro livro = palavra.getLivros().get(livroId);
							if (livro == null) {
								livro = new Livro();
								livro.setId(livroId);	
							}
							
							livro.getLinhas().add(lineNumber);
							palavra.getLivros().put(livroId, livro);
						}
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
