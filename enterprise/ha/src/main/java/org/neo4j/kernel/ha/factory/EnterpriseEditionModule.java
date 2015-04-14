package org.neo4j.kernel.ha.factory;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.logging.Logging;

public class EnterpriseEditionModule
    extends EditionModule
{
    public EnterpriseEditionModule( PlatformModule platformModule )
    {

    }


    protected void doAfterRecoveryAndStartup( String editionName, DependencyResolver resolver, boolean isMaster )
    {
        super.doAfterRecoveryAndStartup( editionName, resolver );

        if ( isMaster )
        {
            new RemoveOrphanConstraintIndexesOnStartup(resolver.resolveDependency( NeoStoreDataSource.class ).getKernel() , resolver.resolveDependency( Logging.class ) ).perform();
        }
    }
}
