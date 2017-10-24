
public class ADLConfig {
	private  String accountFQDN; 
    private  String clientId;
    private  String authTokenEndpoint;
    private  String clientKey;
	private  String tenantId;
    public String getAccountFQDN() {
		return accountFQDN;
	}
	public ADLConfig setAccountName(String accountName) {
		this.accountFQDN = accountName+".azuredatalakestore.net";
		return this;
	}
	public String getClientId() {
		return clientId;
	}
	public ADLConfig setClientId(String clientId) {
		this.clientId = clientId;
		return this;
	}
	public String getAuthTokenEndpoint() {
		return authTokenEndpoint;
	}
	
	public ADLConfig setTenantId(String tenantId) {
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
	public ADLConfig setClientKey(String clientKey) {
		this.clientKey = clientKey;
		return this;
	} 
	public ADLConfig build(){
		return this;
	}
}
