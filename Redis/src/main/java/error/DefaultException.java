package error;

/**
 * 默认的异常
 * @author wzt
 *
 */
public class DefaultException extends Exception{

	private static final long serialVersionUID = 1L;

	private String title ; 
	private String details ;
	
	public DefaultException(String title, String details) {
		super();
		this.title = title;
		this.details = details;
	}
	
	@Override
	public String toString() {
		return "DefaultException [title=" + title + ", details=" + details + "]";
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getDetails() {
		return details;
	}
	public void setDetails(String details) {
		this.details = details;
	} 
	
}

