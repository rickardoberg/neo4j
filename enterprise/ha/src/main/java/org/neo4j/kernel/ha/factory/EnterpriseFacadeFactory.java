package org.neo4j.kernel.ha.factory;

import java.util.Map;

import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class EnterpriseFacadeFactory
    extends GraphDatabaseFacadeFactory
{
    @Override
    public GraphDatabaseFacade newFacade( Map<String, String> params, Dependencies dependencies, GraphDatabaseFacade
            graphDatabaseFacade )
    {
        return super.newFacade( params, dependencies, graphDatabaseFacade );
    }

    @Override
    protected EditionModule createEdition( PlatformModule platformModule )
    {
        return new EnterpriseEditionModule(platformModule);
    }


}
