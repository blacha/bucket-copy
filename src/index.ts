import { fsa } from '@chunkd/fs';
import { FsAwsS3 } from '@chunkd/source-aws';
import { AwsCredentials } from '@chunkd/source-aws-v2';
import S3 from 'aws-sdk/clients/s3';
import pLimit from 'p-limit';
import { log } from '@linzjs/tracing';
import { SharedIniFileCredentials } from 'aws-sdk';

const Q = pLimit(10);

// Use the default profile and assume a role
fsa.register(`s3://linz-baseamps`, AwsCredentials.fsFromRole('arn:...'));

// Use a AWS Profile
const credentials = new SharedIniFileCredentials({ profile: 'some-profile' });
fsa.register(`s3://linz-baseamps`, new FsAwsS3(new S3({ credentials })));

// Default all other S3 profiles
fsa.register(`s3://`, new FsAwsS3(new S3()));

export async function copyFiles(source: string, target: string): Promise<void> {
  const promises: Promise<unknown>[] = [];
  for await (const file of fsa.details(source)) {
    if (file.size === 0 || file.isDirectory) continue;

    // Can filter here
    // Only include tiffs
    // if (!file.path.endsWith('.tiff')) continue

    const baseName = file.path.slice(source.length);
    const targetPath = fsa.join(target, baseName);

    promises.push(
      Q(async () => {
        const head = await fsa.head(targetPath);
        if (head != null && head.size === file.size) {
          log.info('Exists', { path: file.path, size: file.size, target });
          // exists
          return;
        }

        log.trace('Copy', { path: file.path, size: file.size, target });
        await fsa.write(targetPath, fsa.stream(file.path));
        log.info('CopyDone', { path: file.path, size: file.size, target });
      }),
    );

    if (promises.length > 100) {
      await Promise.all(promises);
      promises.length = 0;
    }
  }
  await Promise.all(promises);
}

copyFiles('s3://linz-basemaps/3857/01', 's3://linz-basemaps-dev/2022-10/test');
