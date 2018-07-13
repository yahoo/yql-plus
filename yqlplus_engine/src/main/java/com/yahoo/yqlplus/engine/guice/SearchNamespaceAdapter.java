/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.guice;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yahoo.yqlplus.engine.ModuleNamespace;
import com.yahoo.yqlplus.engine.ModuleType;
import com.yahoo.yqlplus.engine.SourceNamespace;
import com.yahoo.yqlplus.engine.SourceType;
import com.yahoo.yqlplus.engine.api.DependencyNotFoundException;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class SearchNamespaceAdapter implements SourceNamespace, ModuleNamespace {
    private final Map<String, SourceType> sourceMap;
    private final Map<String, ModuleType> moduleMap;
    private final Map<String, SourceNamespace> sourceNamespaceMap;
    private final Map<String, ModuleNamespace> moduleNamespaceMap;
    private final Set<SourceNamespace> sourceNamespaceSet;
    private final Set<ModuleNamespace> moduleNamespaceSet;

    private String keyFor(List<String> path) {
        return Joiner.on('.').join(path);
    }

    @Inject
    SearchNamespaceAdapter(Map<String, SourceType> sourceMap, Map<String, ModuleType> moduleMap, Map<String, SourceNamespace> sourceNamespaceMap, Map<String, ModuleNamespace> moduleNamespaceMap, Set<SourceNamespace> sourceNamespaceSet, Set<ModuleNamespace> moduleNamespaceSet) {
        this.sourceMap = sourceMap;
        this.moduleMap = moduleMap;
        this.sourceNamespaceMap = sourceNamespaceMap;
        this.moduleNamespaceMap = moduleNamespaceMap;
        this.sourceNamespaceSet = sourceNamespaceSet;
        this.moduleNamespaceSet = moduleNamespaceSet;
    }

    private static class SuffixMatch<T> {
        List<String> suffix;
        T module;

        public SuffixMatch(List<String> suffix, T module) {
            this.suffix = suffix;
            this.module = module;
        }
    }

    @Override
    public ModuleType findModule(Location location, List<String> modulePath) {
        String key = keyFor(modulePath);
        if(moduleMap.containsKey(key)) {
            return moduleMap.get(key);
        }
        SuffixMatch<ModuleNamespace> prefixSearch = prefixSearch(moduleNamespaceMap, modulePath);
        if(prefixSearch != null) {
            return prefixSearch.module.findModule(location, prefixSearch.suffix);
        }
        for(ModuleNamespace moduleNamespace : moduleNamespaceSet) {
            try {
                ModuleType moduleType = moduleNamespace.findModule(location, modulePath);
                if (moduleType != null) {
                    return moduleType;
                }
            } catch(DependencyNotFoundException ignored) {

            }
        }
        return null;
    }

    private <T> SuffixMatch<T> prefixSearch(Map<String, T> map, List<String> path) {
        for(int i = path.size()-1; i > 0; i--) {
            List<String> suffix = path.subList(i,  path.size());
            String prefix = keyFor(path.subList(0, i));
            if(map.containsKey(prefix)) {
                return new SuffixMatch<>(suffix, map.get(prefix));
            }
        }
        return null;
    }

    @Override
    public SourceType findSource(Location location, List<String> path) {
        String key = keyFor(path);
        if(sourceMap.containsKey(key)) {
            return sourceMap.get(key);
        }
        SuffixMatch<SourceNamespace> prefixSearch = prefixSearch(sourceNamespaceMap, path);
        if(prefixSearch != null) {
            return prefixSearch.module.findSource(location, prefixSearch.suffix);
        }
        for(SourceNamespace moduleNamespace : sourceNamespaceSet) {
            try {
                SourceType moduleType = moduleNamespace.findSource(location, path);
                if (moduleType != null) {
                    return moduleType;
                }
            } catch(DependencyNotFoundException ignored) {

            }
        }
        return null;
    }
}
