/**
 * Support for Server Sent Events for use with Spring Framework's
 * {@link org.springframework.web.client.RestClient}.
 *
 * <p>The package is null-marked: types are non-null unless annotated
 * {@link org.jspecify.annotations.Nullable}. That includes the item type of a Source,
 * which the implementation relies on, since it uses {@code null} internally to mean
 * "no item"; a {@code Source<@Nullable String>} is therefore not supported.
 */
@NullMarked
package org.springframework.experimental.streamsource.web.client.support;

import org.jspecify.annotations.NullMarked;
