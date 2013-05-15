package similarityjoindataformat;

public class DataObject {

	public int size;
	public boolean isVisited;
	
	public DataObject(){
		this.size=-1;
		this.isVisited=false;
	}
	
	public DataObject(int x, boolean visited){
		this.size=x;
		this.isVisited= visited;
	}
	
	public void setSize(int size){
		this.size=size;
	}
	
	public int getSize(){
		return this.size;
	}
	
	public void setVisited(boolean visited){
		this.isVisited=visited;
	}
	
	public boolean getVisted(){
		return this.isVisited;
	}
}
