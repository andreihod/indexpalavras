package app;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Palavra {
	private Integer id;
	private Map<Integer, Livro> livros = new ConcurrentHashMap<>();

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Map<Integer, Livro> getLivros() {
		return livros;
	}

	public void setLivros(Map<Integer, Livro> livros) {
		this.livros = livros;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Palavra other = (Palavra) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
