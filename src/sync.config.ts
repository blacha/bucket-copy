import { fsa } from '@chunkd/fs';
import { FsAwsS3 } from '@chunkd/source-aws';
// import { AwsCredentials } from '@chunkd/source-aws-v2';
import S3 from 'aws-sdk/clients/s3';
import pLimit from 'p-limit';
import { log } from '@linzjs/tracing';
// import AWS from 'aws-sdk';
import { createGunzip } from 'zlib';
import { Readable } from 'stream';
import { basename } from 'path';

const Q = pLimit(10);

// // Use the default profile and assume a role
// fsa.register(`s3://linz-basemaps`, AwsCredentials.fsFromRole('arn:...'));

// // Use a AWS Profile
// const credentials = new AWS.SharedIniFileCredentials({ profile: 'some-profile' });
// fsa.register(`s3://linz-basemaps`, new FsAwsS3(new S3({ credentials })));

// Default all other S3 profiles
fsa.register(`s3://`, new FsAwsS3(new S3()));

function streamToBuffer(stream?: Readable): Promise<Buffer | undefined> {
  if (stream == null) return Promise.resolve(undefined);
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

export async function syncFromConfig(target: string): Promise<void> {
  const buf = await streamToBuffer(fsa.stream('s3://linz-basemaps/config/config-latest.json.gz').pipe(createGunzip()));

  const cfg = JSON.parse(buf.toString());

  const promises: Promise<unknown>[] = [];
  for (const img of cfg.imagery) {
    if (img.projection !== 3857) continue;

    const imgId = img.id.slice(3);
    const targetPath = fsa.joinAll(target, String(img.projection), img.name, imgId);

    const existingFiles = await fsa.toArray(fsa.details(targetPath));
    log.info('Check:Imagery', { name: img.name, id: img.id, target: targetPath, existing: existingFiles.length });
    const sourceFiles = new Set<string>(img.files.map((f) => f.name + '.tiff'));

    for (const file of existingFiles) {
      const fileName = basename(file.path);
      console.log(fileName);
      sourceFiles.delete(file.path);
    }

    for (const file of sourceFiles) {
      const sourceFile = fsa.join(img.uri, file);
      const targetFile = fsa.join(targetPath, file);
      promises.push(
        Q(async () => {
          log.trace('Copy:Start', { path: sourceFile, target: targetFile });
          await fsa.write(targetFile, fsa.stream(sourceFile));
          log.info('Copy:Done', { path: sourceFile, target: targetFile });
        }),
      );
    }
    if (promises.length > 100) {
      await Promise.all(promises);
      promises.length = 0;
    }
  }
  await Promise.all(promises);
}

syncFromConfig('s3://linz-basemaps-dev/2022-10/test');
