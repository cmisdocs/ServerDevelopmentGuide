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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.chemistry.opencmis.commons.definitions.MutableDocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.MutableFolderTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.MutablePropertyDefinition;
import org.apache.chemistry.opencmis.commons.definitions.MutableTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionContainer;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionList;
import org.apache.chemistry.opencmis.commons.enums.CmisVersion;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.server.CallContext;
import org.apache.chemistry.opencmis.server.support.TypeDefinitionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the type definitions for all FileShare repositories.
 */
public class FileBridgeTypeManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileBridgeTypeManager.class);

    private static final String NAMESPACE = "http://chemistry.apache.org/opencmis/fileshare";

    private final TypeDefinitionFactory typeDefinitionFactory;
    private final Map<String, TypeDefinition> typeDefinitions;

    public FileBridgeTypeManager() {
        // set up TypeDefinitionFactory
        typeDefinitionFactory = TypeDefinitionFactory.newInstance();
        typeDefinitionFactory.setDefaultNamespace(NAMESPACE);
        typeDefinitionFactory.setDefaultControllableAcl(false);
        typeDefinitionFactory.setDefaultControllablePolicy(false);
        typeDefinitionFactory.setDefaultQueryable(false);
        typeDefinitionFactory.setDefaultFulltextIndexed(false);
        typeDefinitionFactory.setDefaultTypeMutability(typeDefinitionFactory.createTypeMutability(false, false, false));

        // set up definitions map
        typeDefinitions = new HashMap<String, TypeDefinition>();

        // add base folder type
        MutableFolderTypeDefinition folderType = typeDefinitionFactory
                .createBaseFolderTypeDefinition(CmisVersion.CMIS_1_1);
        removeQueryableAndOrderableFlags(folderType);
        typeDefinitions.put(folderType.getId(), folderType);

        // add base document type
        MutableDocumentTypeDefinition documentType = typeDefinitionFactory
                .createBaseDocumentTypeDefinition(CmisVersion.CMIS_1_1);
        removeQueryableAndOrderableFlags(documentType);
        typeDefinitions.put(documentType.getId(), documentType);
    }

    /**
     * Adds a type definition.
     */
    public synchronized void addTypeDefinition(TypeDefinition type) {
        if (type == null) {
            throw new IllegalArgumentException("Type must be set!");
        }

        if (type.getId() == null || type.getId().trim().length() == 0) {
            throw new IllegalArgumentException("Type must have a valid id!");
        }

        if (type.getParentTypeId() == null || type.getParentTypeId().trim().length() == 0) {
            throw new IllegalArgumentException("Type must have a valid parent id!");
        }

        TypeDefinition parentType = typeDefinitions.get(type.getParentTypeId());
        if (parentType == null) {
            throw new IllegalArgumentException("Parent type doesn't exist!");
        }

        MutableTypeDefinition newType = typeDefinitionFactory.copy(type, true);

        // copy parent type property definitions and mark them as inherited
        for (PropertyDefinition<?> propDef : parentType.getPropertyDefinitions().values()) {
            MutablePropertyDefinition<?> basePropDef = typeDefinitionFactory.copy(propDef);
            basePropDef.setIsInherited(true);
            newType.addPropertyDefinition(basePropDef);
        }

        typeDefinitions.put(newType.getId(), newType);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Added type '{}'.", type.getId());
        }
    }

    /**
     * Removes the queryable and orderable flags from the property definitions
     * of a type definition because this implementations does neither support
     * queries nor can order objects.
     */
    private void removeQueryableAndOrderableFlags(MutableTypeDefinition type) {
        for (PropertyDefinition<?> propDef : type.getPropertyDefinitions().values()) {
            MutablePropertyDefinition<?> mutablePropDef = (MutablePropertyDefinition<?>) propDef;
            mutablePropDef.setIsQueryable(false);
            mutablePropDef.setIsOrderable(false);
        }
    }

    /**
     * Returns the internal type definition.
     */
    public synchronized TypeDefinition getInternalTypeDefinition(String typeId) {
        return typeDefinitions.get(typeId);
    }

    /**
     * Returns all internal type definitions.
     */
    public synchronized Collection<TypeDefinition> getInternalTypeDefinitions() {
        return typeDefinitions.values();
    }

    // --- service methods ---

    public TypeDefinition getTypeDefinition(CallContext context, String typeId) {
        TypeDefinition type = typeDefinitions.get(typeId);
        if (type == null) {
            throw new CmisObjectNotFoundException("Type '" + typeId + "' is unknown!");
        }

        return typeDefinitionFactory.copy(type, true, context.getCmisVersion());
    }

    public TypeDefinitionList getTypeChildren(CallContext context, String typeId, Boolean includePropertyDefinitions,
            BigInteger maxItems, BigInteger skipCount) {
        return typeDefinitionFactory.createTypeDefinitionList(typeDefinitions, typeId, includePropertyDefinitions,
                maxItems, skipCount, context.getCmisVersion());
    }

    public List<TypeDefinitionContainer> getTypeDescendants(CallContext context, String typeId, BigInteger depth,
            Boolean includePropertyDefinitions) {
        return typeDefinitionFactory.createTypeDescendants(typeDefinitions, typeId, depth, includePropertyDefinitions,
                context.getCmisVersion());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (TypeDefinition type : typeDefinitions.values()) {
            sb.append('[');
            sb.append(type.getId());
            sb.append(" (");
            sb.append(type.getBaseTypeId().value());
            sb.append(")]");
        }

        return sb.toString();
    }
}
