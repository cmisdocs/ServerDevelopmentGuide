/*
 * Copyright 2013 Florian Müller & Jay Brown
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * This code is based on the Apache Chemistry OpenCMIS FileShare project
 * <http://chemistry.apache.org/java/developing/repositories/dev-repositories-fileshare.html>.
 *
 * It is part of a training exercise and not intended for production use!
 *
 */
package org.example.cmis.server;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.chemistry.opencmis.commons.data.Acl;
import org.apache.chemistry.opencmis.commons.data.AllowableActions;
import org.apache.chemistry.opencmis.commons.data.BulkUpdateObjectIdAndChangeToken;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.ExtensionsData;
import org.apache.chemistry.opencmis.commons.data.FailedToDeleteData;
import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderContainer;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderList;
import org.apache.chemistry.opencmis.commons.data.ObjectList;
import org.apache.chemistry.opencmis.commons.data.ObjectParentData;
import org.apache.chemistry.opencmis.commons.data.Properties;
import org.apache.chemistry.opencmis.commons.data.RenditionData;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionList;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ObjectListImpl;
import org.apache.chemistry.opencmis.commons.impl.server.AbstractCmisService;
import org.apache.chemistry.opencmis.commons.server.CallContext;
import org.apache.chemistry.opencmis.commons.spi.Holder;
import org.apache.chemistry.opencmis.server.support.wrapper.CallContextAwareCmisService;

/**
 * FileShare Service implementation.
 */
public class FileBridgeCmisService extends AbstractCmisService implements CallContextAwareCmisService {

	private final FileBridgeRepositoryManager repositoryManager;
	private CallContext context;

	public FileBridgeCmisService(
			final FileBridgeRepositoryManager repositoryManager) {
		this.repositoryManager = repositoryManager;
	}

	// --- Call Context ---

	/**
	 * Sets the call context.
	 * 
	 * This method should only be called by the service factory.
	 */
	public void setCallContext(CallContext context) {
		this.context = context;
	}

	/**
	 * Gets the call context.
	 */
	public CallContext getCallContext() {
		return context;
	}

	/**
	 * Gets the repository for the current call.
	 */
	public FileBridgeRepository getRepository() {
		return repositoryManager.getRepository(getCallContext()
				.getRepositoryId());
	}

	// --- repository service ---

	@Override
	public RepositoryInfo getRepositoryInfo(String repositoryId,
			ExtensionsData extension) {
		for (FileBridgeRepository fsr : repositoryManager.getRepositories()) {
			if (fsr.getRepositoryId().equals(repositoryId)) {
				return fsr.getRepositoryInfo(getCallContext());
			}
		}

		throw new CmisObjectNotFoundException("Unknown repository '"
				+ repositoryId + "'!");
	}

	@Override
	public List<RepositoryInfo> getRepositoryInfos(ExtensionsData extension) {
		List<RepositoryInfo> result = new ArrayList<RepositoryInfo>();

		for (FileBridgeRepository fsr : repositoryManager.getRepositories()) {
			result.add(fsr.getRepositoryInfo(getCallContext()));
		}

		return result;
	}

	@Override
	public TypeDefinitionList getTypeChildren(String repositoryId,
			String typeId, Boolean includePropertyDefinitions,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		return getRepository().getTypeChildren(getCallContext(), typeId,
				includePropertyDefinitions, maxItems, skipCount);
	}

	@Override
	public TypeDefinition getTypeDefinition(String repositoryId, String typeId,
			ExtensionsData extension) {
		return getRepository().getTypeDefinition(getCallContext(), typeId);
	}

	// @Override
	// public List<TypeDefinitionContainer> getTypeDescendants(String
	// repositoryId, String typeId, BigInteger depth,
	// Boolean includePropertyDefinitions, ExtensionsData extension) {
	// return getRepository().getTypeDescendants(getCallContext(), typeId,
	// depth, includePropertyDefinitions);
	// }

	// --- navigation service ---

	@Override
	public ObjectInFolderList getChildren(String repositoryId, String folderId,
			String filter, String orderBy, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, BigInteger maxItems,
			BigInteger skipCount, ExtensionsData extension) {
		return getRepository().getChildren(getCallContext(), folderId, filter,
				includeAllowableActions, includePathSegment, maxItems,
				skipCount, this);
	}

	@Override
	public List<ObjectInFolderContainer> getDescendants(String repositoryId,
			String folderId, BigInteger depth, String filter,
			Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, ExtensionsData extension) {
		return getRepository().getDescendants(getCallContext(), folderId,
				depth, filter, includeAllowableActions, includePathSegment,
				this, false);
	}

	@Override
	public ObjectData getFolderParent(String repositoryId, String folderId,
			String filter, ExtensionsData extension) {
		return getRepository().getFolderParent(getCallContext(), folderId,
				filter, this);
	}

	@Override
	public List<ObjectInFolderContainer> getFolderTree(String repositoryId,
			String folderId, BigInteger depth, String filter,
			Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, ExtensionsData extension) {
		return getRepository().getDescendants(getCallContext(), folderId,
				depth, filter, includeAllowableActions, includePathSegment,
				this, true);
	}

	@Override
	public List<ObjectParentData> getObjectParents(String repositoryId,
			String objectId, String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includeRelativePathSegment, ExtensionsData extension) {
		return getRepository().getObjectParents(getCallContext(), objectId,
				filter, includeAllowableActions, includeRelativePathSegment,
				this);
	}

	@Override
	public ObjectList getCheckedOutDocs(String repositoryId, String folderId,
			String filter, String orderBy, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		ObjectListImpl result = new ObjectListImpl();
		result.setHasMoreItems(false);
		result.setNumItems(BigInteger.ZERO);
		List<ObjectData> emptyList = Collections.emptyList();
		result.setObjects(emptyList);

		return result;
	}

	// --- object service ---

	@Override
	public String create(String repositoryId, Properties properties,
			String folderId, ContentStream contentStream,
			VersioningState versioningState, List<String> policies,
			ExtensionsData extension) {
		ObjectData object = getRepository().create(getCallContext(),
				properties, folderId, contentStream, versioningState, this);

		return object.getId();
	}

	@Override
	public String createDocument(String repositoryId, Properties properties,
			String folderId, ContentStream contentStream,
			VersioningState versioningState, List<String> policies,
			Acl addAces, Acl removeAces, ExtensionsData extension) {
		return getRepository().createDocument(getCallContext(), properties,
				folderId, contentStream, versioningState);
	}

	@Override
	public String createDocumentFromSource(String repositoryId,
			String sourceId, Properties properties, String folderId,
			VersioningState versioningState, List<String> policies,
			Acl addAces, Acl removeAces, ExtensionsData extension) {
		return getRepository().createDocumentFromSource(getCallContext(),
				sourceId, properties, folderId, versioningState);
	}

	@Override
	public String createFolder(String repositoryId, Properties properties,
			String folderId, List<String> policies, Acl addAces,
			Acl removeAces, ExtensionsData extension) {
		return getRepository().createFolder(getCallContext(), properties,
				folderId);
	}

	@Override
	public void deleteObjectOrCancelCheckOut(String repositoryId,
			String objectId, Boolean allVersions, ExtensionsData extension) {
		getRepository().deleteObject(getCallContext(), objectId);
	}

	@Override
	public FailedToDeleteData deleteTree(String repositoryId, String folderId,
			Boolean allVersions, UnfileObject unfileObjects,
			Boolean continueOnFailure, ExtensionsData extension) {
		return getRepository().deleteTree(getCallContext(), folderId,
				continueOnFailure);
	}

	@Override
	public AllowableActions getAllowableActions(String repositoryId,
			String objectId, ExtensionsData extension) {
		return getRepository().getAllowableActions(getCallContext(), objectId);
	}

	@Override
	public ContentStream getContentStream(String repositoryId, String objectId,
			String streamId, BigInteger offset, BigInteger length,
			ExtensionsData extension) {
		return getRepository().getContentStream(getCallContext(), objectId,
				offset, length);
	}

	@Override
	public ObjectData getObject(String repositoryId, String objectId,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
		return getRepository().getObject(getCallContext(), objectId, null,
				filter, includeAllowableActions, includeAcl, this);
	}

	@Override
	public ObjectData getObjectByPath(String repositoryId, String path,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
		return getRepository().getObjectByPath(getCallContext(), path, filter,
				includeAllowableActions, includeAcl, this);
	}

	@Override
	public Properties getProperties(String repositoryId, String objectId,
			String filter, ExtensionsData extension) {
		ObjectData object = getRepository().getObject(getCallContext(),
				objectId, null, filter, false, false, this);
		return object.getProperties();
	}

	@Override
	public List<RenditionData> getRenditions(String repositoryId,
			String objectId, String renditionFilter, BigInteger maxItems,
			BigInteger skipCount, ExtensionsData extension) {
		return Collections.emptyList();
	}

	@Override
	public void moveObject(String repositoryId, Holder<String> objectId,
			String targetFolderId, String sourceFolderId,
			ExtensionsData extension) {
		getRepository().moveObject(getCallContext(), objectId, targetFolderId,
				this);
	}

	@Override
	public void setContentStream(String repositoryId, Holder<String> objectId,
			Boolean overwriteFlag, Holder<String> changeToken,
			ContentStream contentStream, ExtensionsData extension) {
		getRepository().changeContentStream(getCallContext(), objectId,
				overwriteFlag, contentStream, false);
	}

	@Override
	public void appendContentStream(String repositoryId,
			Holder<String> objectId, Holder<String> changeToken,
			ContentStream contentStream, boolean isLastChunk,
			ExtensionsData extension) {
		getRepository().changeContentStream(getCallContext(), objectId, true,
				contentStream, true);
	}

	@Override
	public void deleteContentStream(String repositoryId,
			Holder<String> objectId, Holder<String> changeToken,
			ExtensionsData extension) {
		getRepository().changeContentStream(getCallContext(), objectId, true,
				null, false);
	}

	@Override
	public void updateProperties(String repositoryId, Holder<String> objectId,
			Holder<String> changeToken, Properties properties,
			ExtensionsData extension) {
		getRepository().updateProperties(getCallContext(), objectId,
				properties, this);
	}

	@Override
	public List<BulkUpdateObjectIdAndChangeToken> bulkUpdateProperties(
			String repositoryId,
			List<BulkUpdateObjectIdAndChangeToken> objectIdAndChangeToken,
			Properties properties, List<String> addSecondaryTypeIds,
			List<String> removeSecondaryTypeIds, ExtensionsData extension) {
		return getRepository().bulkUpdateProperties(getCallContext(),
				objectIdAndChangeToken, properties, this);
	}

	// --- versioning service ---

	@Override
	public List<ObjectData> getAllVersions(String repositoryId,
			String objectId, String versionSeriesId, String filter,
			Boolean includeAllowableActions, ExtensionsData extension) {
		ObjectData theVersion = getRepository().getObject(getCallContext(),
				objectId, versionSeriesId, filter, includeAllowableActions,
				false, this);

		return Collections.singletonList(theVersion);
	}

	@Override
	public ObjectData getObjectOfLatestVersion(String repositoryId,
			String objectId, String versionSeriesId, Boolean major,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
		return getRepository().getObject(getCallContext(), objectId,
				versionSeriesId, filter, includeAllowableActions, includeAcl,
				this);
	}

	@Override
	public Properties getPropertiesOfLatestVersion(String repositoryId,
			String objectId, String versionSeriesId, Boolean major,
			String filter, ExtensionsData extension) {
		ObjectData object = getRepository().getObject(getCallContext(),
				objectId, versionSeriesId, filter, false, false, null);

		return object.getProperties();
	}

	// --- ACL service ---

	@Override
	public Acl getAcl(String repositoryId, String objectId,
			Boolean onlyBasicPermissions, ExtensionsData extension) {
		return getRepository().getAcl(getCallContext(), objectId);
	}

	// --- discovery service ---

	@Override
	public ObjectList query(String repositoryId, String statement,
			Boolean searchAllVersions, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		return getRepository().query(getCallContext(), statement,
				includeAllowableActions, maxItems, skipCount, this);
	}
}
