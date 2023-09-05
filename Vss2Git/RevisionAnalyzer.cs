/* Copyright 2009 HPDI, LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Hpdi.VssLogicalLib;
using Hpdi.VssPhysicalLib;

namespace Hpdi.Vss2Git
{
    /// <summary>
    /// Enumerates revisions in a VSS database.
    /// </summary>
    /// <author>Trevor Robinson</author>
    class RevisionAnalyzer : Worker
    {
        private string excludeFiles;
        public string ExcludeFiles
        {
            get { return excludeFiles; }
            set { excludeFiles = value; }
        }

        private readonly VssDatabase database;
        public VssDatabase Database
        {
            get { return database; }
        }

        private readonly LinkedList<VssProject> rootProjects = new LinkedList<VssProject>();
        public IEnumerable<VssProject> RootProjects
        {
            get { return rootProjects; }
        }

        private readonly LinkedList<VssProject> parentProjects = new LinkedList<VssProject>();
        public IEnumerable<VssProject> ParentProjects
        {
            get { return parentProjects; }
        }


        private readonly SortedDictionary<DateTime, ICollection<Revision>> sortedRevisions =
            new SortedDictionary<DateTime, ICollection<Revision>>();
        public SortedDictionary<DateTime, ICollection<Revision>> SortedRevisions
        {
            get { return sortedRevisions; }
        }

        private readonly HashSet<string> processedFiles = new HashSet<string>();
        public HashSet<string> ProcessedFiles
        {
            get { return processedFiles; }
        }

        private readonly HashSet<string> destroyedFiles = new HashSet<string>();
        public HashSet<string> DestroyedFiles
        {
            get { return destroyedFiles; }
        }

        private int projectCount;
        public int ProjectCount
        {
            get { return Thread.VolatileRead(ref projectCount); }
        }

        private int fileCount;
        public int FileCount
        {
            get { return Thread.VolatileRead(ref fileCount); }
        }

        private int revisionCount;
        public int RevisionCount
        {
            get { return Thread.VolatileRead(ref revisionCount); }
        }

        public RevisionAnalyzer(WorkQueue workQueue, Logger logger, VssDatabase database)
            : base(workQueue, logger)
        {
            this.database = database;
        }

        public bool IsDestroyed(string physicalName)
        {
            return destroyedFiles.Contains(physicalName);
        }

        public void AddItem(VssProject rootProj, VssProject project) // project is "$"
        {
            if (rootProj == null)
            {
                throw new ArgumentNullException("project");
            }
            else if (rootProj.Database != database)
            {
                throw new ArgumentException("Project database mismatch", "project");
            }
            
            rootProjects.AddLast(rootProj);

            PathMatcher exclusionMatcher = null;
            if (!string.IsNullOrEmpty(excludeFiles))
            {
                var excludeFileArray = excludeFiles.Split(
                    new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                exclusionMatcher = new PathMatcher(excludeFileArray);
            }

            workQueue.AddLast(delegate(object work)
            {
                logger.WriteSectionSeparator();
                LogStatus(work, "Building revision list");

                logger.WriteLine("Root project: {0}", rootProj.Path);
                logger.WriteLine("Recurse from project: {0} to get inherited labels", project.Path);
                logger.WriteLine("Excluded files: {0}", excludeFiles);

                int excludedProjects = 0;
                int excludedFiles = 0;
                var stopwatch = Stopwatch.StartNew();
                VssUtil.RecurseItems(project,
                    delegate(VssProject subproject)
                    {
                        if (workQueue.IsAborting)
                        {
                            return RecursionStatus.Abort;
                        }

                        var path = subproject.Path;
                        if (exclusionMatcher != null && exclusionMatcher.Matches(path))
                        {
                            logger.WriteLine("Excluding project {0}", path);
                            ++excludedProjects;
                            return RecursionStatus.Skip;
                        }

                        bool isParent=false;
                        int lp = subproject.Path.Length; // Length of current  being migrated
                        int lr = rootProj.Path.Length; //  Length of root being migrated
                     
                        if (!subproject.Path.Contains(rootProj.Path))
                        {
                            if (!rootProj.Path.Contains(subproject.Path))
                            {
                                // A path we are not interested in
                                logger.WriteLine("Skipping project {0}", subproject.Path);
                                return RecursionStatus.Skip;
                            }
                        }


                        if (lp < lr)
                        {
                            logger.WriteLine("Parent project {0}", subproject.Path);
                            isParent = true;
                        }
                        else
                        {
                            logger.WriteLine("Processing project {0}", subproject.Path);
                        }



                        ProcessItem(subproject, path, exclusionMatcher, isParent);
                        ++projectCount;

                        return RecursionStatus.Continue;
                    },
                    delegate(VssProject subproject, VssFile file)
                    {
                        if (workQueue.IsAborting)
                        {
                            return RecursionStatus.Abort;
                        }

                        var path = file.GetPath(subproject);
                        if (exclusionMatcher != null && exclusionMatcher.Matches(path))
                        {
                            logger.WriteLine("Excluding file {0}", path);
                            ++excludedFiles;
                            return RecursionStatus.Skip;
                        }

                        // only process shared files once (projects are never shared)
                        if (!processedFiles.Contains(file.PhysicalName))
                        {
                            processedFiles.Add(file.PhysicalName);
                            ProcessItem(file, path, exclusionMatcher, false);
                            ++fileCount;
                        }

                        return RecursionStatus.Continue;
                    });
                stopwatch.Stop();

                logger.WriteSectionSeparator();
                logger.WriteLine("Analysis complete in {0:HH:mm:ss}", new DateTime(stopwatch.ElapsedTicks));
                logger.WriteLine("Projects: {0} ({1} excluded)", projectCount, excludedProjects);
                logger.WriteLine("Files: {0} ({1} excluded)", fileCount, excludedFiles);
                logger.WriteLine("Revisions: {0}", revisionCount);
            });
        }

        private void ProcessItem(VssItem item, string path, PathMatcher exclusionMatcher, bool isParent)
        {
            try
            {
                foreach (VssRevision vssRevision in item.Revisions)
                {
                    var actionType = vssRevision.Action.Type;
                    var namedAction = vssRevision.Action as VssNamedAction;
                    if (namedAction != null)
                    {
                        if (actionType == VssActionType.Destroy)
                        {
                            // track destroyed files so missing history can be anticipated
                            // (note that Destroy actions on shared files simply delete
                            // that copy, so destroyed files can't be completely ignored)
                            destroyedFiles.Add(namedAction.Name.PhysicalName);
                        }

                        if (isParent)
                        {
                            if (actionType != VssActionType.Label)
                            {
                                continue; // Ignore all except labels for parents
                            }
                            else
                            {
                                logger.WriteSectionSeparator();
                                logger.WriteLine("ProcessItem of type label for parent {0}  {1}", item.ItemName, vssRevision.Action.Type.ToString());
                            }
                        }
                        var targetPath = path + VssDatabase.ProjectSeparator + namedAction.Name.LogicalName;
                        if (exclusionMatcher != null && exclusionMatcher.Matches(targetPath))
                        {
                            // project action targets an excluded file
                            continue;
                        }
                    }

                    Revision revision = new Revision(vssRevision.DateTime,
                        vssRevision.User, item.ItemName, vssRevision.Version,
                        vssRevision.Comment, vssRevision.Action);

                    ICollection<Revision> revisionSet;
                    if (!sortedRevisions.TryGetValue(vssRevision.DateTime, out revisionSet))
                    {
                        revisionSet = new LinkedList<Revision>();
                        sortedRevisions[vssRevision.DateTime] = revisionSet;
                    }
                    revisionSet.Add(revision);
                    ++revisionCount;
                }
            }
            catch (RecordException e)
            {
                var message = string.Format("Failed to read revisions for {0} ({1}): {2}",
                    path, item.PhysicalName, ExceptionFormatter.Format(e));
                LogException(e, message);
                ReportError(message);
            }
        }
    }
}
