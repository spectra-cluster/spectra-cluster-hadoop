package uk.ac.ebi.pride.spectracluster.hadoop.util;

import java.io.InputStream;

/**
 * Abstracts the notion of the object holding parameters
 *
 * @author Steve Lewis
 */
public interface IParameterHolder extends org.systemsbiology.hadoop.IStreamOpener {
    /**
     * for testing this has no data
     */
    public static final IParameterHolder NULL_PARAMETER_HOLDER = new NullParameterHolder();

    /**
     * return all keys as an array
     *
     * @return !null key array
     */
    public String[] getParameterKeys();
    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public String getParameter(String key);

    /**
     * get all keys of the foro key , key 1, key 2 ...
     *
     * @param key !null key
     * @return non-null array
     */
    public String[] getIndexedParameters(String key);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public Integer getIntParameter(String key);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public Boolean getBooleanParameter(String key);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public Float getFloatParameter(String key);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public Double getDoubleParameter(String key);


    /**
     * return an enum from the value of the parameter
     *
     * @param key !nulkl key
     * @param cls !null expected class
     * @param <T> type of enum
     * @return possibly null value - null says parameter does not exist
     */
    public <T extends Enum<T>> T getEnumParameter(String key, Class<T> cls);


    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     * @defaultValue what to set when the parameter is null
     */
    public String getParameter(String key, String defaultValue);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     * @defaultValue what to set when the parameter is null
     */
    public Integer getIntParameter(String key, int defaultValue);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     * @defaultValue what to set when the parameter is null
     */
    public Boolean getBooleanParameter(String key, boolean defaultValue);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     * @defaultValue what to set when the parameter is null
     */
    public Float getFloatParameter(String key, float defaultValue);

    /**
     * access a parameter from a parameter map
     *
     * @param key !null key
     * @return possibly null parameter
     * @defaultValue what to set when the parameter is null
     */
    public Double getDoubleParameter(String key, double defaultValue);


    /**
     * return an enum from the value of the parameter
     *
     * @param key !nulkl key
     * @param cls !null expected class
     * @param <T> type of enum
     * @return possibly null value - null says parameter does not exist
     * @defaultValue what to set when the parameter is null
     */
    public <T extends Enum<T>> T getEnumParameter(String key, Class<T> cls, T defaultValue);


    public static class NullParameterHolder implements IParameterHolder {
        private NullParameterHolder() {
        }

        /**
         * open a file from a string
         *
         * @param fileName  string representing the file
         * @param otherData any other required data
         * @return possibly null stream
         */
        @Override
        public InputStream open(final String fileName, final Object... otherData) {
            return null;
        }

        /**
         * return all keys as an array
         *
         * @return !null key array
         */
        @Override
        public String[] getParameterKeys() {
            return new String[0];
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key !null key
         * @return possibly null parameter
         */
        @Override
        public String getParameter(final String key) {
            return null;
        }

        /**
         * get all keys of the foro key , key 1, key 2 ...
         *
         * @param key !null key
         * @return non-null array
         */
        @Override
        public String[] getIndexedParameters(final String key) {
            return new String[0];
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key !null key
         * @return possibly null parameter
         */
        @Override
        public Integer getIntParameter(final String key) {
            return null;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key !null key
         * @return possibly null parameter
         */
        @Override
        public Boolean getBooleanParameter(final String key) {
            return null;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key !null key
         * @return possibly null parameter
         */
        @Override
        public Float getFloatParameter(final String key) {
            return null;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key !null key
         * @return possibly null parameter
         */
        @Override
        public Double getDoubleParameter(final String key) {
            return null;
        }

        /**
         * return an enum from the value of the parameter
         *
         * @param key !nulkl key
         * @param cls !null expected class
         * @param <T> type of enum
         * @return possibly null value - null says parameter does not exist
         */
        @Override
        public <T extends Enum<T>> T getEnumParameter(final String key, final Class<T> cls) {
            return null;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key          !null key
         * @param defaultValue what to set when the parameter is null
         * @return possibly null parameter
         *         c
         */
        @Override
        public String getParameter(final String key, final String defaultValue) {
            return defaultValue;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key          !null key
         * @param defaultValue what to set when the parameter is null
         * @return possibly null parameter
         */
        @Override
        public Integer getIntParameter(final String key, final int defaultValue) {
            return defaultValue;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key          !null key
         * @param defaultValue what to set when the parameter is null
         * @return possibly null parameter
         */
        @Override
        public Boolean getBooleanParameter(final String key, final boolean defaultValue) {
            return defaultValue;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key          !null key
         * @param defaultValue what to set when the parameter is null
         * @return possibly null parameter
         */
        @Override
        public Float getFloatParameter(final String key, final float defaultValue) {
            return defaultValue;
        }

        /**
         * access a parameter from a parameter map
         *
         * @param key          !null key
         * @param defaultValue what to set when the parameter is null
         * @return possibly null parameter
         */
        @Override
        public Double getDoubleParameter(final String key, final double defaultValue) {
            return defaultValue;
        }

        /**
         * return an enum from the value of the parameter
         *
         * @param key          !null key
         * @param cls          !null expected class
         * @param <T>          type of enum
         * @param defaultValue what to set when the parameter is null
         * @return possibly null value - null says parameter does not exist
         */
        @Override
        public <T extends Enum<T>> T getEnumParameter(final String key, final Class<T> cls, final T defaultValue) {
            return defaultValue;
        }
    }
}
