package org.neo4j.graphdb.factory;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.SettingsTest;

// @Ignore("Only use this to test new output. Normally disabled")
public class SettingsResourceBundleTest
{
    @Test
    public void testSettingsResourceBundle()
    {
        SettingsResourceBundle bundle = new SettingsResourceBundle( SettingsTest.class );

        for ( String name : bundle.keySet() )
        {
            System.out.printf( "%s=%s\n", name, bundle.getString( name ) );
        }
    }
}
