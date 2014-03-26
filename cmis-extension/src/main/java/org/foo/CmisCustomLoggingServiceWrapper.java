package org.foo;

import java.math.BigInteger;
import javax.servlet.http.HttpServletRequest;
import org.apache.chemistry.opencmis.commons.data.ExtensionsData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderList;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.server.CallContext;
import org.apache.chemistry.opencmis.commons.server.CmisService;
import org.apache.chemistry.opencmis.server.support.wrapper.AbstractCmisServiceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of a minimal Cmis Custom Service Wrapper (logging example)
 * 
 * Add the following ** to the repo.properties to have framework hook into chain
 * The number at the key is the position in the wrapper stack. Lower numbers are
 * outer wrappers, higher numbers are inner wrappers.
 *
 * ** add the following line to your repo.properties file in your servers war 
 * servicewrapper.1=org.apache.chemistry.opencmis.server.support.CmisCustomLoggingServiceWrapper
 * 
 * See the frameworks SimpleLoggingCmisServiceWrapper for a more generic and
 * complete example
 * 
 */
public class CmisCustomLoggingServiceWrapper extends AbstractCmisServiceWrapper {

	// slf4j example
	private static final Logger LOG = LoggerFactory.getLogger(CmisCustomLoggingServiceWrapper.class);
	
	// provide constructor
	public CmisCustomLoggingServiceWrapper(CmisService service) {
		super(service);

	}

	/**
	 * slf logging version with dual output to console and slf 
	 */
	protected void slflog(String operation, String repositoryId) {
		if (repositoryId == null) {
			repositoryId = "<none>";
		}

		HttpServletRequest request = (HttpServletRequest) getCallContext().get(CallContext.HTTP_SERVLET_REQUEST);
		String userAgent = request.getHeader("User-Agent");
		if (userAgent == null) {
			userAgent = "<unknown>";
		}

		String binding = getCallContext().getBinding();

		LOG.info("Operation: {}, Repository ID: {}, Binding: {}, User Agent: {}", operation, repositoryId, binding,
				userAgent);
		
		// also dump to console for testing
		String result =
		String.format("Operation: %s, Repository ID: %s, Binding: %s, User Agent: %s", 
				operation, repositoryId, binding, userAgent);
		System.out.println(result);
	}
	
	@Override
	public ObjectInFolderList getChildren(String repositoryId, String folderId, String filter, String orderBy,
			Boolean includeAllowableActions, IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {

		slflog("getChildren override from customer Chameleon module --------------", repositoryId);
		long startTime = System.currentTimeMillis();

		CallContext sharedContext = this.getCallContext();

		// Get the native domain object from the call context if one is shared by the vendor (example only)
		// Your CMIS vendor's documentation must expose the name of any shared objects they place here for extensions.
		// Object objShared = sharedContext.get("shared_key_name_from_vendor");
	
		ObjectInFolderList retVal = getWrappedService().getChildren(repositoryId, folderId, filter, orderBy, includeAllowableActions,
				includeRelationships, renditionFilter, includePathSegment, maxItems, skipCount, extension);
	
		// dual log output in case logger not configured
		LOG.info("[CmisCustomServiceWrapper] Exiting method getChildren. time (ms):" + (System.currentTimeMillis() - startTime));
		System.out.println("[CmisCustomServiceWrapper] Exiting method getChildren. time (ms):" + (System.currentTimeMillis() - startTime));
		return retVal;
	}

}
