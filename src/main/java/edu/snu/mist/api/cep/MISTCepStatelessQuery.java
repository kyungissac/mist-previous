/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.api.cep;

import java.util.ArrayList;
import java.util.List;

/**
 * A class which contains information about MISTCepStateless query.
 */
public final class MISTCepStatelessQuery {

  private final CepInput cepInput;
  private final List<CepStatelessRule> cepStatelessRules;

  /**
   * Creates an immutable stateless query.
   */
  public MISTCepStatelessQuery(final CepInput cepInput,
                               final List<CepStatelessRule> cepStatelessRules) {
    this.cepInput = cepInput;
    this.cepStatelessRules = cepStatelessRules;
  }

  /**
   * @return input for this query.
   */
  public CepInput getCepInput() {
    return cepInput;
  }

  /**
   * @return list of all cep stateless rules.
   */
  public List<CepStatelessRule> getCepStatelessRules() {
    return this.cepStatelessRules;
  }

  /**
   * Builder for MISTCepStatelessQuery.
   */
  public static class Builder {
    private CepInput cepInput;
    private final List<CepStatelessRule> cepStatelessRules;

    /**
     * Creates a new builder.
     */
    public Builder() {
      this.cepInput = null;
      this.cepStatelessRules = new ArrayList<>();
    }

    /**
     * Sets the input for this stateless cep query.
     * @param inputParam parameter for input
     * @return builder
     */
    public Builder input(final CepInput inputParam) {
      this.cepInput = inputParam;
      return this;
    }

    /**
     * Add a stateless rule.
     * @param cepStatelessRule a target rule
     * @return buidler
     */
    public Builder addStatelessRule(final CepStatelessRule cepStatelessRule) {
      cepStatelessRules.add(cepStatelessRule);
      return this;
    }

    /**
     * Creates an immutable stateless cep query.
     * @return
     */
    public MISTCepStatelessQuery build() {
      if (cepInput == null || cepStatelessRules.size() == 0) {
        throw new IllegalStateException("Cep input or cep stateless rules are not defined!");
      }
      return new MISTCepStatelessQuery(cepInput, cepStatelessRules);
    }
  }
}