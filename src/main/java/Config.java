
public class Config {
	private  String accountFQDN; 
    private  String clientId;
    private  String authTokenEndpoint;
    private  String clientKey;
	private  String tenantId;
    public String getAccountFQDN() {
		return accountFQDN;
	}
	public Config setAccountName(String accountName) {
		this.accountFQDN = accountName+".azuredatalakestore.net";
		return this;
	}
	public String getClientId() {
		return clientId;
	}
	public Config setClientId(String clientId) {
		this.clientId = clientId;
		return this;
	}
	public String getAuthTokenEndpoint() {
		return authTokenEndpoint;
	}
	
	public Config setTenantId(String tenantId) {
		this.tenantId=tenantId;
		this.authTokenEndpoint = "https://login.windows.net/"+tenantId+"/oauth2/token";
		return this;
	}
	public String getTenantId(){
		return this.tenantId;
	}
	
	public String getClientKey() {
		return clientKey;
	}
	public Config setClientKey(String clientKey) {
		this.clientKey = clientKey;
		return this;
	} 
	public Config build(){
		return this;
	}
}
