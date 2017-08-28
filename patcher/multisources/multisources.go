package multisources

/*
 * MultiSources Patcher will stream the patched version of the file to output from multiple sources, since it works
 * strictly in order, it cannot patch the local file directly (since it might overwrite a block needed later), so there
 * would have to be a final copy once the patching was done.
 */

