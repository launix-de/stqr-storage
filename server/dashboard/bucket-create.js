// SPDX-License-Identifier: GPL-3.0-only
// Administrative creation remains format-neutral. Native format binding and any
// initialization are delegated to the plugin host after the bare bucket exists.
export async function finishBucketCreation({
  connection,
  plugins,
  progress,
  signal,
  onCreated = () => {},
}) {
  const { bucket, owner } = progress;
  if (!progress.created) {
    if (bucket.format) {
      if (!plugins) throw Error("Format-Plugins sind nicht verfügbar.");
      await plugins.module(bucket.format);
    }
    await connection.createBucket(bucket, progress.id);
    progress.created = true;
  }
  onCreated();
  if (!progress.initialized) {
    if (bucket.format)
      await plugins.initialize(bucket, {
        signal,
        initializationID: progress.initializationID,
        state: progress.initializationState,
      });
    progress.initialized = true;
  }
  if (owner && !progress.granted) {
    await connection.grantBucket(bucket.id, owner, 15, progress.grantID);
    progress.granted = true;
  }
  return bucket;
}
