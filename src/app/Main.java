package app;

public class Main {
	
	public static void main(String[] args) {
		try {
			new IndexPalavras();
		} catch (Exception e) {
			System.err.println("Ocorreu um erro: " + e.getMessage());
			e.printStackTrace();
		}
	}

}
