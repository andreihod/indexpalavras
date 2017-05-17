package app;

import java.util.List;

public class Palavra {
	private int id;
	private int livroId;
	private List<Integer> linhas;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getLivroId() {
		return livroId;
	}

	public void setLivroId(int livroId) {
		this.livroId = livroId;
	}

	public List<Integer> getLinhas() {
		return linhas;
	}

	public void setLinhas(List<Integer> linhas) {
		this.linhas = linhas;
	}

	@Override
	public String toString() {
		return "Palavra [id=" + id + ", livroId=" + livroId + ", linhas=" + linhas + "]";
	}

}
